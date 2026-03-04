/**
 * Script de recálculo em massa de PnL por posição (modelo compra → venda)
 * para todos os swap_events já gravados no MySQL.
 *
 * Uso:
 *   npm run recalculate-pnl
 *
 * MODELO DE POSIÇÃO UNIVERSAL (sem BASE_TOKENS fixos):
 *
 *   Para cada swap A → B:
 *
 *   CASO 1 — VENDA (A tem posição aberta com custo em B):
 *     PnL = recebido_B − (avg_cost_B × qty_A_vendida)
 *     PnL expresso em B
 *
 *   CASO 2 — COMPRA (B não tem posição, ou tem posição com custo em A):
 *     Abre/aumenta posição em B com custo em A (VWAP)
 *
 *   CASO 3 — SWAP sem PnL (A tem posição com custo em outro token ≠ B):
 *     Fecha posição de A (sem PnL), abre posição em B com custo em A
 *
 * Todos os cálculos usam Decimal.js (precisão arbitrária).
 * Seguro para rodar múltiplas vezes (idempotente).
 */

import 'dotenv/config';
import Decimal from 'decimal.js';
import { query, execute, closePool } from '../database/connection';
import { updateAllKolMetrics } from '../services/metrics-service';
import { logger } from '../utils/logger';

Decimal.set({ precision: 36, rounding: Decimal.ROUND_DOWN });

const WALLET_CONCURRENCY = 5;
const ZERO = new Decimal(0);

// ─────────────────────────────────────────────────────────────────────────────
// Conversão RAW → Decimal
// ─────────────────────────────────────────────────────────────────────────────
function rawToDecimal(rawStr: string, decimals: number): Decimal {
  if (!rawStr || rawStr === '0') return ZERO;
  return new Decimal(rawStr).div(new Decimal(10).pow(decimals));
}

// ─────────────────────────────────────────────────────────────────────────────
// Tipos
// ─────────────────────────────────────────────────────────────────────────────
interface SwapRow {
  id: number;
  tx_hash: string;
  timestamp: Date;
  token_in_address: string;
  token_in_symbol: string;
  token_in_amount: string;
  token_out_address: string;
  token_out_symbol: string;
  token_out_amount: string;
  token_in_decimals: number | null;
  token_out_decimals: number | null;
}

interface Position {
  tokenAddress: string;
  tokenSymbol: string;
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

  const positions = new Map<string, Position>();

  const updates: Array<{
    id: number;
    swapType: 'buy' | 'sell' | 'swap';
    pnlBase: string | null;
    pnlPct: string | null;
    pnlBaseToken: string | null;
    pnlBaseSymbol: string | null;
    isWin: number | null;
  }> = [];

  for (const swap of swaps) {
    const inAddr  = swap.token_in_address.toLowerCase();
    const outAddr = swap.token_out_address.toLowerCase();

    const inDecimals  = swap.token_in_decimals  ?? 18;
    const outDecimals = swap.token_out_decimals ?? 18;
    const amtIn  = rawToDecimal(String(swap.token_in_amount),  inDecimals);
    const amtOut = rawToDecimal(String(swap.token_out_amount), outDecimals);

    const posIn  = positions.get(inAddr);
    const posOut = positions.get(outAddr);

    let swapType: 'buy' | 'sell' | 'swap' = 'swap';
    let pnlBase:       string | null = null;
    let pnlPct:        string | null = null;
    let pnlBaseToken:  string | null = null;
    let pnlBaseSymbol: string | null = null;
    let isWin:         number | null = null;

    // CASO 1: VENDA — token_in tem posição com custo em token_out
    if (posIn && posIn.baseTokenAddress === outAddr && posIn.qtyOpen.gt(ZERO)) {
      swapType = 'sell';

      const costProporcional = posIn.avgCostBase.mul(amtIn);
      const pnlDecimal       = amtOut.minus(costProporcional);
      const pnlPctDecimal    = costProporcional.gt(ZERO)
        ? pnlDecimal.div(costProporcional).mul(100)
        : ZERO;

      pnlBase       = pnlDecimal.toFixed(18);
      pnlPct        = pnlPctDecimal.toFixed(4);
      pnlBaseToken  = outAddr;
      pnlBaseSymbol = swap.token_out_symbol;
      isWin         = pnlDecimal.gt(ZERO) ? 1 : 0;

      const qtyAfter = Decimal.max(ZERO, posIn.qtyOpen.minus(amtIn));
      const costAfter = posIn.qtyOpen.gt(ZERO)
        ? posIn.costBasisBase.mul(qtyAfter).div(posIn.qtyOpen)
        : ZERO;

      if (qtyAfter.lte(ZERO)) {
        positions.delete(inAddr);
      } else {
        posIn.qtyOpen       = qtyAfter;
        posIn.costBasisBase = costAfter;
        posIn.avgCostBase   = qtyAfter.gt(ZERO) ? costAfter.div(qtyAfter) : ZERO;
      }

    // CASO 2: COMPRA — B não tem posição ou tem posição com custo em A
    } else if (!posOut || posOut.baseTokenAddress === inAddr) {
      swapType = 'buy';

      if (!posOut) {
        positions.set(outAddr, {
          tokenAddress:     outAddr,
          tokenSymbol:      swap.token_out_symbol,
          baseTokenAddress: inAddr,
          baseTokenSymbol:  swap.token_in_symbol,
          qtyOpen:          amtOut,
          costBasisBase:    amtIn,
          avgCostBase:      amtOut.gt(ZERO) ? amtIn.div(amtOut) : ZERO,
          openedAt:         new Date(swap.timestamp),
        });
      } else {
        const newQty  = posOut.qtyOpen.plus(amtOut);
        const newCost = posOut.costBasisBase.plus(amtIn);
        posOut.qtyOpen       = newQty;
        posOut.costBasisBase = newCost;
        posOut.avgCostBase   = newQty.gt(ZERO) ? newCost.div(newQty) : ZERO;
      }

    // CASO 3: SWAP sem PnL calculável
    } else {
      swapType = 'swap';

      // Fecha posição de A (custo em outro token)
      if (posIn && posIn.qtyOpen.gt(ZERO)) {
        const qtyAfter = Decimal.max(ZERO, posIn.qtyOpen.minus(amtIn));
        const costAfter = posIn.qtyOpen.gt(ZERO)
          ? posIn.costBasisBase.mul(qtyAfter).div(posIn.qtyOpen)
          : ZERO;
        if (qtyAfter.lte(ZERO)) {
          positions.delete(inAddr);
        } else {
          posIn.qtyOpen       = qtyAfter;
          posIn.costBasisBase = costAfter;
          posIn.avgCostBase   = qtyAfter.gt(ZERO) ? costAfter.div(qtyAfter) : ZERO;
        }
      }

      // Fecha posição de B se tiver base diferente de A, abre nova
      if (posOut && posOut.baseTokenAddress !== inAddr) {
        positions.delete(outAddr);
      }
      const existingOut = positions.get(outAddr);
      if (!existingOut) {
        positions.set(outAddr, {
          tokenAddress:     outAddr,
          tokenSymbol:      swap.token_out_symbol,
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

    updates.push({ id: swap.id, swapType, pnlBase, pnlPct, pnlBaseToken, pnlBaseSymbol, isWin });
  }

  // Gravar UPDATEs no banco
  for (const u of updates) {
    await execute(
      `UPDATE swap_events
       SET swap_type       = ?,
           pnl_base        = ?,
           pnl_pct         = ?,
           pnl_base_token  = ?,
           pnl_base_symbol = ?,
           is_win          = ?
       WHERE id = ?`,
      [u.swapType, u.pnlBase, u.pnlPct, u.pnlBaseToken, u.pnlBaseSymbol, u.isWin, u.id]
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

  // Limpar tabela positions
  logger.info('Limpando tabela positions...');
  await execute('DELETE FROM positions', []);
  logger.info('Tabela positions limpa.');

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
        if (pos.qtyOpen.lte(ZERO)) continue;
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
