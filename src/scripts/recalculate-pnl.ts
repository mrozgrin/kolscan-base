/**
 * Script de recálculo em massa de PnL por posição (modelo compra → venda)
 * para todos os swap_events já gravados no MySQL.
 *
 * Uso:
 *   npm run recalculate-pnl
 *
 * Todos os cálculos numéricos usam Decimal.js (precisão arbitrária) para
 * evitar erros de arredondamento de float64 em valores wei (> 2^53).
 */

import 'dotenv/config';
import Decimal from 'decimal.js';
import { query, execute, closePool } from '../database/connection';
import { updateAllKolMetrics } from '../services/metrics-service';
import { logger } from '../utils/logger';

// Precisão global: 36 dígitos significativos (suficiente para qualquer token ERC-20)
Decimal.set({ precision: 36, rounding: Decimal.ROUND_DOWN });

// ─────────────────────────────────────────────────────────────────────────────
// Tokens considerados "moeda base" na blockchain Base
// ─────────────────────────────────────────────────────────────────────────────
const BASE_TOKENS = new Map<string, string>([
  ['0x4200000000000000000000000000000000000006', 'WETH'],
  ['0x833589fcd6edb6e08f4c7c32d4f71b54bda02913', 'USDC'],
  ['0xfde4c96c8593536e31f229ea8f37b2ada2699bb2', 'USDT'],
  ['0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca', 'USDbC'],
]);

const WALLET_CONCURRENCY = 5;

// ─────────────────────────────────────────────────────────────────────────────
// Conversão segura de RAW (wei string) → Decimal
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Converte um valor RAW inteiro (string) para Decimal em unidades humanas.
 * Ex: rawToDecimal('5703410781985153175', 18) → Decimal('5.703410781985153175')
 */
function rawToDecimal(rawStr: string, decimals: number): Decimal {
  if (!rawStr || rawStr === '0') return new Decimal(0);
  // Decimal.js aceita strings diretamente — sem perda de precisão
  return new Decimal(rawStr).div(new Decimal(10).pow(decimals));
}

// ─────────────────────────────────────────────────────────────────────────────
// Tipos internos
// ─────────────────────────────────────────────────────────────────────────────

interface SwapRow {
  id: number;
  tx_hash: string;
  timestamp: Date;
  token_in_address: string;
  token_in_symbol: string;
  token_in_amount: string;   // RAW inteiro como string (DECIMAL(65,0))
  token_out_address: string;
  token_out_symbol: string;
  token_out_amount: string;  // RAW inteiro como string (DECIMAL(65,0))
  token_in_decimals: number | null;
  token_out_decimals: number | null;
}

interface Position {
  baseTokenAddress: string;
  baseTokenSymbol: string;
  qtyOpen: Decimal;
  costBasisBase: Decimal;
  avgCostBase: Decimal;
  openedAt: Date;
}

// ─────────────────────────────────────────────────────────────────────────────
// Lógica principal por wallet
// ─────────────────────────────────────────────────────────────────────────────

async function reprocessWallet(
  walletAddress: string
): Promise<Map<string, Position>> {
  const swaps = await query<SwapRow>(
    `SELECT
       se.id, se.tx_hash, se.timestamp,
       se.token_in_address,  se.token_in_symbol,  se.token_in_amount,
       se.token_out_address, se.token_out_symbol, se.token_out_amount,
       ti.decimals  AS token_in_decimals,
       to_.decimals AS token_out_decimals
     FROM swap_events se
     LEFT JOIN tokens ti  ON ti.address  = se.token_in_address
     LEFT JOIN tokens to_ ON to_.address = se.token_out_address
     WHERE se.wallet_address = ?
     ORDER BY se.timestamp ASC, se.id ASC`,
    [walletAddress]
  );

  // Posições abertas em memória: token_address → Position
  const positions = new Map<string, Position>();

  // Batch de UPDATEs para executar no final
  const updates: Array<{
    id: number;
    swapType: 'buy' | 'sell' | 'swap';
    pnlBase: string | null;       // string para preservar precisão no INSERT
    pnlBaseToken: string | null;
    pnlBaseSymbol: string | null;
    isWin: number | null;
  }> = [];

  const ZERO = new Decimal(0);

  for (const swap of swaps) {
    const inAddr  = swap.token_in_address.toLowerCase();
    const outAddr = swap.token_out_address.toLowerCase();

    // Converter RAW → Decimal (precisão total)
    const inDecimals  = swap.token_in_decimals  ?? 18;
    const outDecimals = swap.token_out_decimals ?? 18;
    const amtIn  = rawToDecimal(String(swap.token_in_amount),  inDecimals);
    const amtOut = rawToDecimal(String(swap.token_out_amount), outDecimals);

    const inIsBase  = BASE_TOKENS.has(inAddr);
    const outIsBase = BASE_TOKENS.has(outAddr);

    let swapType: 'buy' | 'sell' | 'swap';
    if      (inIsBase && !outIsBase)  swapType = 'buy';
    else if (!inIsBase && outIsBase)  swapType = 'sell';
    else                              swapType = 'swap';

    let pnlBase:       string | null = null;
    let pnlBaseToken:  string | null = null;
    let pnlBaseSymbol: string | null = null;
    let isWin:         number | null = null;

    if (swapType === 'buy') {
      // ── COMPRA: abre ou aumenta posição ──────────────────────────────────
      const baseAddr   = inAddr;
      const baseSymbol = BASE_TOKENS.get(baseAddr) ?? swap.token_in_symbol;

      const pos = positions.get(outAddr);
      if (!pos) {
        positions.set(outAddr, {
          baseTokenAddress: baseAddr,
          baseTokenSymbol:  baseSymbol,
          qtyOpen:          amtOut,
          costBasisBase:    amtIn,
          avgCostBase:      amtOut.gt(ZERO) ? amtIn.div(amtOut) : ZERO,
          openedAt:         new Date(swap.timestamp),
        });
      } else {
        const newQty  = pos.qtyOpen.plus(amtOut);
        const newCost = pos.costBasisBase.plus(amtIn);
        pos.qtyOpen       = newQty;
        pos.costBasisBase = newCost;
        pos.avgCostBase   = newQty.gt(ZERO) ? newCost.div(newQty) : ZERO;
      }

    } else if (swapType === 'sell') {
      // ── VENDA: fecha ou reduz posição e calcula PnL ───────────────────────
      const baseAddr   = outAddr;
      const baseSymbol = BASE_TOKENS.get(baseAddr) ?? swap.token_out_symbol;

      const pos = positions.get(inAddr);

      if (pos && pos.qtyOpen.gt(ZERO)) {
        // PnL = valor recebido em base − custo proporcional
        const costProporcional = pos.avgCostBase.mul(amtIn);
        const pnlDecimal       = amtOut.minus(costProporcional);

        pnlBase       = pnlDecimal.toFixed(18);  // string com 18 casas decimais
        pnlBaseToken  = baseAddr;
        pnlBaseSymbol = baseSymbol;
        isWin         = pnlDecimal.gt(ZERO) ? 1 : 0;

        // Reduzir posição proporcionalmente
        const qtyAfter = Decimal.max(ZERO, pos.qtyOpen.minus(amtIn));
        const costAfter = pos.qtyOpen.gt(ZERO)
          ? pos.costBasisBase.mul(qtyAfter).div(pos.qtyOpen)
          : ZERO;

        if (qtyAfter.lte(ZERO)) {
          positions.delete(inAddr);
        } else {
          pos.qtyOpen       = qtyAfter;
          pos.costBasisBase = costAfter;
          pos.avgCostBase   = qtyAfter.gt(ZERO) ? costAfter.div(qtyAfter) : ZERO;
        }
      } else {
        // Venda sem compra registrada (posição anterior ao início da indexação)
        pnlBase       = null;
        pnlBaseToken  = baseAddr;
        pnlBaseSymbol = baseSymbol;
        isWin         = null;
      }

    } else {
      // ── SWAP meme→meme ────────────────────────────────────────────────────
      const pos = positions.get(inAddr);
      if (pos && pos.qtyOpen.gt(ZERO)) {
        const qtyAfter = Decimal.max(ZERO, pos.qtyOpen.minus(amtIn));
        const costAfter = pos.qtyOpen.gt(ZERO)
          ? pos.costBasisBase.mul(qtyAfter).div(pos.qtyOpen)
          : ZERO;

        if (qtyAfter.lte(ZERO)) {
          positions.delete(inAddr);
        } else {
          pos.qtyOpen       = qtyAfter;
          pos.costBasisBase = costAfter;
          pos.avgCostBase   = qtyAfter.gt(ZERO) ? costAfter.div(qtyAfter) : ZERO;
        }
      }

      const existingOut = positions.get(outAddr);
      if (!existingOut) {
        positions.set(outAddr, {
          baseTokenAddress: inAddr,
          baseTokenSymbol:  swap.token_in_symbol,
          qtyOpen:          amtOut,
          costBasisBase:    amtIn,
          avgCostBase:      amtOut.gt(ZERO) ? amtIn.div(amtOut) : ZERO,
          openedAt:         new Date(swap.timestamp),
        });
      } else {
        const newQty  = existingOut.qtyOpen.plus(amtOut);
        const newCost = existingOut.costBasisBase.plus(amtIn);
        existingOut.qtyOpen       = newQty;
        existingOut.costBasisBase = newCost;
        existingOut.avgCostBase   = newQty.gt(ZERO) ? newCost.div(newQty) : ZERO;
      }
    }

    updates.push({ id: swap.id, swapType, pnlBase, pnlBaseToken, pnlBaseSymbol, isWin });
  }

  // Gravar todos os UPDATEs no banco
  for (const u of updates) {
    await execute(
      `UPDATE swap_events
       SET swap_type       = ?,
           pnl_base        = ?,
           pnl_base_token  = ?,
           pnl_base_symbol = ?,
           is_win          = ?
       WHERE id = ?`,
      [u.swapType, u.pnlBase, u.pnlBaseToken, u.pnlBaseSymbol, u.isWin, u.id]
    );
  }

  return positions;
}

// ─────────────────────────────────────────────────────────────────────────────
// Função principal
// ─────────────────────────────────────────────────────────────────────────────

async function recalculatePnl(): Promise<void> {
  logger.info('=== recalculate-pnl: iniciando recálculo completo de PnL ===');

  const totalSwapsResult = await query<{ cnt: number }>(
    'SELECT COUNT(*) AS cnt FROM swap_events'
  );
  const totalSwaps = Number(totalSwapsResult[0]?.cnt) || 0;
  logger.info(`Total de swap_events no banco: ${totalSwaps}`);

  const wallets = await query<{ wallet_address: string; cnt: number }>(
    `SELECT wallet_address, COUNT(*) AS cnt
     FROM swap_events
     GROUP BY wallet_address
     ORDER BY cnt DESC`
  );
  logger.info(`Total de wallets a processar: ${wallets.length}`);

  // Passo 1: Limpar tabela positions
  logger.info('Limpando tabela positions...');
  await execute('DELETE FROM positions', []);
  logger.info('Tabela positions limpa.');

  // Passo 2: Reprocessar cada wallet
  let walletsProcessed = 0;
  let swapsProcessed   = 0;

  for (let i = 0; i < wallets.length; i += WALLET_CONCURRENCY) {
    const batch = wallets.slice(i, i + WALLET_CONCURRENCY);

    const results = await Promise.all(
      batch.map(async ({ wallet_address, cnt }) => {
        const openPositions = await reprocessWallet(wallet_address);
        return { wallet_address, cnt, openPositions };
      })
    );

    for (const { wallet_address, cnt, openPositions } of results) {
      for (const [tokenAddress, pos] of openPositions.entries()) {
        if (pos.qtyOpen.lte(0)) continue;
        await execute(
          `INSERT INTO positions
             (wallet_address, token_address, base_token_address, base_token_symbol,
              qty_open, cost_basis_base, avg_cost_base, opened_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             qty_open           = VALUES(qty_open),
             cost_basis_base    = VALUES(cost_basis_base),
             avg_cost_base      = VALUES(avg_cost_base),
             base_token_address = VALUES(base_token_address),
             base_token_symbol  = VALUES(base_token_symbol)`,
          [
            wallet_address,
            tokenAddress,
            pos.baseTokenAddress,
            pos.baseTokenSymbol,
            pos.qtyOpen.toFixed(18),
            pos.costBasisBase.toFixed(18),
            pos.avgCostBase.toFixed(18),
            pos.openedAt,
          ]
        );
      }

      walletsProcessed++;
      swapsProcessed += cnt;
    }

    logger.info(
      `Progresso: ${walletsProcessed}/${wallets.length} wallets | ` +
      `${swapsProcessed}/${totalSwaps} swaps processados`
    );
  }

  logger.info('Recálculo de PnL concluído. Atualizando kol_metrics...');
  await updateAllKolMetrics();
  logger.info('=== recalculate-pnl: concluído com sucesso ===');
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  try {
    await recalculatePnl();
  } catch (error) {
    logger.error('recalculate-pnl falhou', { error: (error as Error).message });
    process.exit(1);
  } finally {
    await closePool();
    process.exit(0);
  }
}

main();
