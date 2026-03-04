/**
 * Script de AMOSTRA — recálculo de PnL para wallets específicas.
 *
 * Uso:
 *   npm run recalculate-pnl-amostra
 *
 * MODELO DE POSIÇÃO UNIVERSAL (sem BASE_TOKENS fixos):
 *
 *   Cada token que uma wallet possui tem uma posição com:
 *     - token_address      : o token acumulado (ex: CHARLES)
 *     - base_token_address : o token gasto para comprá-lo (ex: VIRTUAL)
 *     - qty_open           : quantidade em carteira
 *     - cost_basis_base    : total gasto em base para comprar
 *     - avg_cost_base      : custo médio por unidade em base
 *
 *   Para cada swap A → B:
 *
 *   CASO 1 — VENDA (A tem posição aberta com custo em B):
 *     PnL = recebido_B − (avg_cost_B × qty_A_vendida)
 *     PnL expresso em B (a moeda base da posição de A)
 *
 *   CASO 2 — COMPRA (B não tem posição, ou tem posição com custo em A):
 *     Abre/aumenta posição em B com custo em A (VWAP)
 *
 *   CASO 3 — TROCA SEM PNL (A tem posição com custo em outro token ≠ B):
 *     Fecha posição de A (sem PnL calculável), abre posição em B com custo em A
 *
 *   CASO 4 — TROCA SEM PNL (B tem posição com custo em outro token ≠ A):
 *     Fecha posição de B (sem PnL), abre nova posição em B com custo em A
 *
 * Todos os cálculos usam Decimal.js (precisão arbitrária).
 */

import 'dotenv/config';
import Decimal from 'decimal.js';
import { query, execute, closePool } from '../database/connection';
import { logger } from '../utils/logger';

Decimal.set({ precision: 36, rounding: Decimal.ROUND_DOWN });

// ─────────────────────────────────────────────────────────────────────────────
// Wallets de amostra
// ─────────────────────────────────────────────────────────────────────────────
const SAMPLE_WALLETS = [
  '0x163d541d0c385042a85292fdba798fb4f5fd3fed',
  '0xb6f1824162f01512213cc692ca87aed0fb3a4ce9',
  '0x6550815de033dc3d92a04dc4eaa2c303dda99ade',
];

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
  pnl_base_atual: string | null;
  swap_type_atual: string | null;
}

interface Position {
  tokenAddress: string;
  tokenSymbol: string;
  baseTokenAddress: string;
  baseTokenSymbol: string;
  qtyOpen: Decimal;
  costBasisBase: Decimal;
  avgCostBase: Decimal;
}

// ─────────────────────────────────────────────────────────────────────────────
// Processamento de uma wallet
// ─────────────────────────────────────────────────────────────────────────────
async function processWalletAmostra(walletAddress: string): Promise<void> {
  console.log('\n' + '='.repeat(100));
  console.log(`WALLET: ${walletAddress}`);
  console.log('='.repeat(100));

  const swaps = await query<SwapRow>(
    `SELECT
       se.id, se.tx_hash, se.timestamp,
       se.token_in_address,  se.token_in_symbol,  se.token_in_amount,
       se.token_out_address, se.token_out_symbol, se.token_out_amount,
       ti.decimals  AS token_in_decimals,
       to_.decimals AS token_out_decimals,
       se.pnl_base  AS pnl_base_atual,
       se.swap_type AS swap_type_atual
     FROM swap_events se
     LEFT JOIN tokens ti  ON ti.address  = se.token_in_address
     LEFT JOIN tokens to_ ON to_.address = se.token_out_address
     WHERE se.wallet_address = ?
     ORDER BY se.timestamp ASC, se.id ASC`,
    [walletAddress]
  );

  if (swaps.length === 0) {
    console.log('  Nenhum swap encontrado para esta wallet.');
    return;
  }

  console.log(`  Total de swaps: ${swaps.length}\n`);

  // Posições em memória: token_address → Position
  const positions = new Map<string, Position>();

  const updates: Array<{
    id: number;
    swapType: 'buy' | 'sell' | 'swap';
    pnlBase: string | null;
    pnlBaseToken: string | null;
    pnlBaseSymbol: string | null;
    isWin: number | null;
  }> = [];

  let totalPnlByBase = new Map<string, Decimal>(); // base_symbol → pnl acumulado
  let totalVendas = 0;
  let vendasComPnl = 0;
  let vendasSemPosicao = 0;

  for (const swap of swaps) {
    const inAddr  = swap.token_in_address.toLowerCase();
    const outAddr = swap.token_out_address.toLowerCase();

    const inDecimals  = swap.token_in_decimals  ?? 18;
    const outDecimals = swap.token_out_decimals ?? 18;
    const amtIn  = rawToDecimal(String(swap.token_in_amount),  inDecimals);
    const amtOut = rawToDecimal(String(swap.token_out_amount), outDecimals);

    const ts = new Date(swap.timestamp).toISOString().replace('T', ' ').substring(0, 19);

    // Posição atual de cada token envolvido
    const posIn  = positions.get(inAddr);   // posição do token que está saindo
    const posOut = positions.get(outAddr);  // posição do token que está entrando

    let swapType: 'buy' | 'sell' | 'swap' = 'swap';
    let pnlBase:       string | null = null;
    let pnlBaseToken:  string | null = null;
    let pnlBaseSymbol: string | null = null;
    let isWin:         number | null = null;

    // ── CASO 1: VENDA ─────────────────────────────────────────────────────────
    // token_in tem posição aberta e a moeda base dessa posição é o token_out
    if (posIn && posIn.baseTokenAddress === outAddr && posIn.qtyOpen.gt(ZERO)) {
      swapType = 'sell';
      totalVendas++;
      vendasComPnl++;

      const costProporcional = posIn.avgCostBase.mul(amtIn);
      const pnlDecimal       = amtOut.minus(costProporcional);

      pnlBase       = pnlDecimal.toFixed(18);
      pnlBaseToken  = outAddr;
      pnlBaseSymbol = swap.token_out_symbol;
      isWin         = pnlDecimal.gt(ZERO) ? 1 : 0;

      // Acumular PnL por moeda base para o resumo
      const prev = totalPnlByBase.get(swap.token_out_symbol) ?? ZERO;
      totalPnlByBase.set(swap.token_out_symbol, prev.plus(pnlDecimal));

      // Reduzir posição
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

      const winStr = pnlDecimal.gt(ZERO) ? '✓ WIN' : '✗ LOSS';
      console.log(`  [${ts}] id=${swap.id} SELL ${amtIn.toFixed(4)} ${swap.token_in_symbol} → ${amtOut.toFixed(6)} ${swap.token_out_symbol}`);
      console.log(`           → custo_prop=${costProporcional.toFixed(6)} ${swap.token_out_symbol} | recebido=${amtOut.toFixed(6)} ${swap.token_out_symbol}`);
      console.log(`           → PnL = ${pnlDecimal.toFixed(8)} ${swap.token_out_symbol}  ${winStr}`);
      console.log(`           → pnl_base ANTES: ${swap.pnl_base_atual ?? 'NULL'} | DEPOIS: ${pnlDecimal.toFixed(8)}`);

    // ── CASO 2: COMPRA — B não tem posição ou tem posição com custo em A ──────
    } else if (!posOut || (posOut.baseTokenAddress === inAddr)) {
      swapType = 'buy';

      if (!posOut) {
        // Nova posição
        positions.set(outAddr, {
          tokenAddress:     outAddr,
          tokenSymbol:      swap.token_out_symbol,
          baseTokenAddress: inAddr,
          baseTokenSymbol:  swap.token_in_symbol,
          qtyOpen:          amtOut,
          costBasisBase:    amtIn,
          avgCostBase:      amtOut.gt(ZERO) ? amtIn.div(amtOut) : ZERO,
        });
      } else {
        // Aumenta posição existente (VWAP)
        const newQty  = posOut.qtyOpen.plus(amtOut);
        const newCost = posOut.costBasisBase.plus(amtIn);
        posOut.qtyOpen       = newQty;
        posOut.costBasisBase = newCost;
        posOut.avgCostBase   = newQty.gt(ZERO) ? newCost.div(newQty) : ZERO;
      }

      const p = positions.get(outAddr)!;
      console.log(`  [${ts}] id=${swap.id} BUY  ${amtIn.toFixed(6)} ${swap.token_in_symbol} → ${amtOut.toFixed(4)} ${swap.token_out_symbol}`);
      console.log(`           → avg_cost=${p.avgCostBase.toFixed(8)} ${swap.token_in_symbol} | posição=${p.qtyOpen.toFixed(4)} ${swap.token_out_symbol}`);

    // ── CASO 3/4: SWAP sem PnL calculável ─────────────────────────────────────
    } else {
      swapType = 'swap';

      // Verifica se há posição de token_in sem moeda base correspondente
      if (posIn && posIn.qtyOpen.gt(ZERO)) {
        totalVendas++;
        vendasSemPosicao++;
        // Fecha posição de A sem PnL (moeda base não coincide com B)
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

      // Fecha posição de B se existir com outra base, e abre nova com custo em A
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
        });
      } else {
        const newQty  = existingOut.qtyOpen.plus(amtOut);
        const newCost = existingOut.costBasisBase.plus(amtIn);
        existingOut.qtyOpen       = newQty;
        existingOut.costBasisBase = newCost;
        existingOut.avgCostBase   = newQty.gt(ZERO) ? newCost.div(newQty) : ZERO;
      }

      console.log(`  [${ts}] id=${swap.id} SWAP ${amtIn.toFixed(4)} ${swap.token_in_symbol} → ${amtOut.toFixed(4)} ${swap.token_out_symbol}`);
      console.log(`           → sem moeda base coincidente — PnL = NULL`);
    }

    updates.push({ id: swap.id, swapType, pnlBase, pnlBaseToken, pnlBaseSymbol, isWin });
  }

  // Resumo da wallet
  console.log('\n' + '-'.repeat(60));
  console.log(`  RESUMO ${walletAddress.substring(0, 10)}...`);
  console.log(`  Total swaps         : ${swaps.length}`);
  console.log(`  Vendas totais       : ${totalVendas}`);
  console.log(`  Vendas com PnL calc : ${vendasComPnl}`);
  console.log(`  Vendas sem PnL      : ${vendasSemPosicao}`);
  if (totalPnlByBase.size > 0) {
    console.log(`  PnL realizado:`);
    for (const [symbol, pnl] of totalPnlByBase.entries()) {
      const winStr = pnl.gt(ZERO) ? '✓' : '✗';
      console.log(`    ${winStr} ${pnl.toFixed(8)} ${symbol}`);
    }
  } else {
    console.log(`  PnL realizado       : nenhum`);
  }
  if (positions.size > 0) {
    console.log(`  Posições abertas    : ${positions.size} token(s)`);
    for (const [, pos] of positions.entries()) {
      console.log(`    ${pos.tokenSymbol}: qty=${pos.qtyOpen.toFixed(4)} | avg_cost=${pos.avgCostBase.toFixed(8)} ${pos.baseTokenSymbol}`);
    }
  }
  console.log('-'.repeat(60));

  // Gravar swap_events no banco
  console.log(`\n  Gravando ${updates.length} atualizações em swap_events...`);
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

  // Gravar posições abertas na tabela positions
  if (positions.size > 0) {
    console.log(`  Gravando ${positions.size} posição(ões) abertas em positions...`);
    for (const [tokenAddress, pos] of positions.entries()) {
      if (pos.qtyOpen.lte(ZERO)) continue;
      await execute(
        `INSERT INTO positions
           (wallet_address, token_address, base_token_address, base_token_symbol,
            qty_open, cost_basis_base, avg_cost_base, opened_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
         ON DUPLICATE KEY UPDATE
           qty_open           = VALUES(qty_open),
           cost_basis_base    = VALUES(cost_basis_base),
           avg_cost_base      = VALUES(avg_cost_base),
           base_token_address = VALUES(base_token_address),
           base_token_symbol  = VALUES(base_token_symbol),
           last_updated       = NOW()`,
        [
          walletAddress,
          tokenAddress,
          pos.baseTokenAddress,
          pos.baseTokenSymbol,
          pos.qtyOpen.toFixed(18),
          pos.costBasisBase.toFixed(18),
          pos.avgCostBase.toFixed(18),
        ]
      );
    }
  }

  console.log(`  ✓ Banco atualizado.`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────
async function main(): Promise<void> {
  logger.info('=== recalculate-pnl-amostra: iniciando ===');
  console.log(`\nProcessando ${SAMPLE_WALLETS.length} wallets de amostra...\n`);

  try {
    for (const wallet of SAMPLE_WALLETS) {
      await processWalletAmostra(wallet.toLowerCase());
    }

    console.log('\n' + '='.repeat(100));
    console.log('AMOSTRA CONCLUÍDA — verifique os resultados acima antes de rodar npm run recalculate-pnl');
    console.log('='.repeat(100) + '\n');

    logger.info('=== recalculate-pnl-amostra: concluído ===');
  } catch (error) {
    logger.error('recalculate-pnl-amostra falhou', { error: (error as Error).message });
    process.exit(1);
  } finally {
    await closePool();
    process.exit(0);
  }
}

main();
