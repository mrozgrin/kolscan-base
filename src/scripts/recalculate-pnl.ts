/**
 * Script de recálculo em massa de PnL por posição (modelo compra → venda)
 * para todos os swap_events já gravados no MySQL.
 *
 * Uso:
 *   npm run recalculate-pnl
 *
 * O que faz:
 *   1. Para cada wallet, lê todos os swap_events em ordem cronológica.
 *   2. Classifica cada swap como 'buy', 'sell' ou 'swap' com base nos
 *      tokens envolvidos (BASE_TOKENS).
 *   3. Reconstrói as posições em memória usando custo médio ponderado (VWAP):
 *      - BUY:  acumula qty e custo na moeda base
 *      - SELL: calcula pnl_base = valor_recebido − (avg_cost × qty_vendida)
 *   4. Atualiza swap_events com:
 *      - swap_type         ('buy' | 'sell' | 'swap')
 *      - pnl_base          (número ou null)
 *      - pnl_base_token    (endereço da moeda base)
 *      - pnl_base_symbol   (símbolo da moeda base)
 *      - is_win            (1 se pnl_base > 0, 0 se <= 0, null se não aplicável)
 *   5. Limpa a tabela positions e a reconstrói com as posições ainda abertas.
 *   6. Recalcula kol_metrics para todas as wallets.
 *
 * Seguro para rodar múltiplas vezes (idempotente).
 * Não apaga nenhum swap_event — apenas atualiza campos.
 *
 * NOTA SOBRE PRECISÃO:
 *   token_in_amount e token_out_amount são gravados no banco como inteiros RAW (wei),
 *   em colunas DECIMAL(65,0). O MySQL retorna esses valores como strings para evitar
 *   perda de precisão. Usamos BigInt para a conversão inicial e só depois convertemos
 *   para float — isso evita erros de arredondamento em valores > 2^53.
 */

import 'dotenv/config';
import { query, execute, closePool } from '../database/connection';
import { updateAllKolMetrics } from '../services/metrics-service';
import { logger } from '../utils/logger';

// ─────────────────────────────────────────────────────────────────────────────
// Tokens considerados "moeda base" na blockchain Base
// ─────────────────────────────────────────────────────────────────────────────
const BASE_TOKENS = new Map<string, string>([
  ['0x4200000000000000000000000000000000000006', 'WETH'],
  ['0x833589fcd6edb6e08f4c7c32d4f71b54bda02913', 'USDC'],
  ['0xfde4c96c8593536e31f229ea8f37b2ada2699bb2', 'USDT'],
  ['0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca', 'USDbC'],
]);

// Número de wallets processadas em paralelo
const WALLET_CONCURRENCY = 5;

// ─────────────────────────────────────────────────────────────────────────────
// Conversão segura de RAW (wei string) → float humano
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Converte um valor RAW (inteiro em string, ex: "5703410781985153175")
 * para float em unidades humanas usando BigInt para preservar precisão.
 *
 * Equivale a: Number(rawStr) / 10^decimals
 * mas sem perder bits significativos em valores > 2^53.
 */
function rawToFloat(rawStr: string, decimals: number): number {
  if (!rawStr || rawStr === '0') return 0;

  try {
    const raw = BigInt(rawStr);
    const divisor = BigInt(10) ** BigInt(decimals);

    // Parte inteira
    const intPart = raw / divisor;
    // Parte fracionária (resto)
    const fracPart = raw % divisor;

    // Combinar: inteiro + fração / 10^decimals
    return Number(intPart) + Number(fracPart) / Number(divisor);
  } catch {
    // Fallback para parseFloat caso o valor não seja um inteiro válido
    return parseFloat(rawStr) / Math.pow(10, decimals);
  }
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
  qtyOpen: number;
  costBasisBase: number;
  avgCostBase: number;
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
    pnlBase: number | null;
    pnlBaseToken: string | null;
    pnlBaseSymbol: string | null;
    isWin: number | null;
  }> = [];

  for (const swap of swaps) {
    const inAddr  = swap.token_in_address.toLowerCase();
    const outAddr = swap.token_out_address.toLowerCase();

    // Converter RAW → float usando BigInt para preservar precisão
    const inDecimals  = swap.token_in_decimals  ?? 18;
    const outDecimals = swap.token_out_decimals ?? 18;
    const amtIn  = rawToFloat(String(swap.token_in_amount),  inDecimals);
    const amtOut = rawToFloat(String(swap.token_out_amount), outDecimals);

    const inIsBase  = BASE_TOKENS.has(inAddr);
    const outIsBase = BASE_TOKENS.has(outAddr);

    let swapType: 'buy' | 'sell' | 'swap';
    if      (inIsBase && !outIsBase)  swapType = 'buy';
    else if (!inIsBase && outIsBase)  swapType = 'sell';
    else                              swapType = 'swap';

    let pnlBase:       number | null = null;
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
          avgCostBase:      amtOut > 0 ? amtIn / amtOut : 0,
          openedAt:         new Date(swap.timestamp),
        });
      } else {
        const newQty  = pos.qtyOpen       + amtOut;
        const newCost = pos.costBasisBase + amtIn;
        pos.qtyOpen       = newQty;
        pos.costBasisBase = newCost;
        pos.avgCostBase   = newQty > 0 ? newCost / newQty : 0;
      }

    } else if (swapType === 'sell') {
      // ── VENDA: fecha ou reduz posição e calcula PnL ───────────────────────
      const baseAddr   = outAddr;
      const baseSymbol = BASE_TOKENS.get(baseAddr) ?? swap.token_out_symbol;

      const pos = positions.get(inAddr);

      if (pos && pos.qtyOpen > 0) {
        const costProporcional = pos.avgCostBase * amtIn;
        pnlBase       = amtOut - costProporcional;
        pnlBaseToken  = baseAddr;
        pnlBaseSymbol = baseSymbol;
        isWin         = pnlBase > 0 ? 1 : 0;

        const qtyAfter  = Math.max(0, pos.qtyOpen - amtIn);
        const costAfter = pos.qtyOpen > 0
          ? pos.costBasisBase * (qtyAfter / pos.qtyOpen)
          : 0;

        if (qtyAfter <= 0) {
          positions.delete(inAddr);
        } else {
          pos.qtyOpen       = qtyAfter;
          pos.costBasisBase = costAfter;
          pos.avgCostBase   = qtyAfter > 0 ? costAfter / qtyAfter : 0;
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
      if (pos && pos.qtyOpen > 0) {
        const qtyAfter  = Math.max(0, pos.qtyOpen - amtIn);
        const costAfter = pos.qtyOpen > 0
          ? pos.costBasisBase * (qtyAfter / pos.qtyOpen)
          : 0;

        if (qtyAfter <= 0) {
          positions.delete(inAddr);
        } else {
          pos.qtyOpen       = qtyAfter;
          pos.costBasisBase = costAfter;
          pos.avgCostBase   = qtyAfter > 0 ? costAfter / qtyAfter : 0;
        }
      }

      const existingOut = positions.get(outAddr);
      if (!existingOut) {
        positions.set(outAddr, {
          baseTokenAddress: inAddr,
          baseTokenSymbol:  swap.token_in_symbol,
          qtyOpen:          amtOut,
          costBasisBase:    amtIn,
          avgCostBase:      amtOut > 0 ? amtIn / amtOut : 0,
          openedAt:         new Date(swap.timestamp),
        });
      } else {
        const newQty  = existingOut.qtyOpen       + amtOut;
        const newCost = existingOut.costBasisBase + amtIn;
        existingOut.qtyOpen       = newQty;
        existingOut.costBasisBase = newCost;
        existingOut.avgCostBase   = newQty > 0 ? newCost / newQty : 0;
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

    // Gravar posições abertas restantes
    for (const { wallet_address, cnt, openPositions } of results) {
      for (const [tokenAddress, pos] of openPositions.entries()) {
        if (pos.qtyOpen <= 0) continue;
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
            pos.qtyOpen,
            pos.costBasisBase,
            pos.avgCostBase,
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
