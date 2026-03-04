/**
 * Script de AMOSTRA — recálculo de PnL para wallets específicas.
 *
 * Uso:
 *   npm run recalculate-pnl-amostra
 *
 * Diferenças em relação ao recalculate-pnl completo:
 *   - Processa apenas as wallets listadas em SAMPLE_WALLETS
 *   - Exibe no terminal um relatório detalhado de cada swap (tipo, amounts, PnL)
 *   - NÃO recalcula kol_metrics ao final (mais rápido para validação)
 *   - Atualiza o banco normalmente — use para validar antes do recálculo completo
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

// ─────────────────────────────────────────────────────────────────────────────
// Tokens base
// ─────────────────────────────────────────────────────────────────────────────
const BASE_TOKENS = new Map<string, string>([
  ['0x4200000000000000000000000000000000000006', 'WETH'],
  ['0x833589fcd6edb6e08f4c7c32d4f71b54bda02913', 'USDC'],
  ['0xfde4c96c8593536e31f229ea8f37b2ada2699bb2', 'USDT'],
  ['0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca', 'USDbC'],
]);

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
  baseTokenAddress: string;
  baseTokenSymbol: string;
  qtyOpen: Decimal;
  costBasisBase: Decimal;
  avgCostBase: Decimal;
}

// ─────────────────────────────────────────────────────────────────────────────
// Processamento de uma wallet com output detalhado
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

  const positions = new Map<string, Position>();
  let totalPnl = ZERO;
  let totalVendas = 0;
  let vendasComPnl = 0;
  let vendasSemPosicao = 0;

  const updates: Array<{
    id: number;
    swapType: 'buy' | 'sell' | 'swap';
    pnlBase: string | null;
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
    let logPnl = '';

    const ts = new Date(swap.timestamp).toISOString().replace('T', ' ').substring(0, 19);

    if (swapType === 'buy') {
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
        });
      } else {
        const newQty  = pos.qtyOpen.plus(amtOut);
        const newCost = pos.costBasisBase.plus(amtIn);
        pos.qtyOpen       = newQty;
        pos.costBasisBase = newCost;
        pos.avgCostBase   = newQty.gt(ZERO) ? newCost.div(newQty) : ZERO;
      }

      const pos2 = positions.get(outAddr)!;
      logPnl = `avg_cost=${pos2.avgCostBase.toFixed(8)} ${baseSymbol} | posição=${pos2.qtyOpen.toFixed(4)} ${swap.token_out_symbol}`;
      console.log(`  [${ts}] id=${swap.id} BUY  ${amtIn.toFixed(6)} ${swap.token_in_symbol} → ${amtOut.toFixed(4)} ${swap.token_out_symbol}`);
      console.log(`           → ${logPnl}`);

    } else if (swapType === 'sell') {
      totalVendas++;
      const baseAddr   = outAddr;
      const baseSymbol = BASE_TOKENS.get(baseAddr) ?? swap.token_out_symbol;

      const pos = positions.get(inAddr);

      if (pos && pos.qtyOpen.gt(ZERO)) {
        vendasComPnl++;
        const costProporcional = pos.avgCostBase.mul(amtIn);
        const pnlDecimal       = amtOut.minus(costProporcional);

        pnlBase       = pnlDecimal.toFixed(18);
        pnlBaseToken  = baseAddr;
        pnlBaseSymbol = baseSymbol;
        isWin         = pnlDecimal.gt(ZERO) ? 1 : 0;
        totalPnl      = totalPnl.plus(pnlDecimal);

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

        const winStr = pnlDecimal.gt(ZERO) ? '✓ WIN' : '✗ LOSS';
        console.log(`  [${ts}] id=${swap.id} SELL ${amtIn.toFixed(4)} ${swap.token_in_symbol} → ${amtOut.toFixed(6)} ${swap.token_out_symbol}`);
        console.log(`           → custo_prop=${costProporcional.toFixed(6)} ${baseSymbol} | recebido=${amtOut.toFixed(6)} ${baseSymbol}`);
        console.log(`           → PnL = ${pnlDecimal.toFixed(8)} ${baseSymbol}  ${winStr}`);
        console.log(`           → pnl_base ANTES: ${swap.pnl_base_atual ?? 'NULL'} | DEPOIS: ${pnlDecimal.toFixed(8)}`);

      } else {
        vendasSemPosicao++;
        pnlBase       = null;
        pnlBaseToken  = baseAddr;
        pnlBaseSymbol = baseSymbol;
        isWin         = null;

        console.log(`  [${ts}] id=${swap.id} SELL ${amtIn.toFixed(4)} ${swap.token_in_symbol} → ${amtOut.toFixed(6)} ${swap.token_out_symbol}`);
        console.log(`           → ⚠ sem posição registrada (compra anterior à indexação) — PnL = NULL`);
      }

    } else {
      console.log(`  [${ts}] id=${swap.id} SWAP ${amtIn.toFixed(4)} ${swap.token_in_symbol} → ${amtOut.toFixed(4)} ${swap.token_out_symbol}`);

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

  // Resumo da wallet
  console.log('\n' + '-'.repeat(60));
  console.log(`  RESUMO ${walletAddress.substring(0, 10)}...`);
  console.log(`  Total swaps         : ${swaps.length}`);
  console.log(`  Vendas totais       : ${totalVendas}`);
  console.log(`  Vendas com PnL calc : ${vendasComPnl}`);
  console.log(`  Vendas sem posição  : ${vendasSemPosicao}`);
  console.log(`  PnL total realizado : ${totalPnl.toFixed(8)} (moedas base)`);
  if (positions.size > 0) {
    console.log(`  Posições abertas    : ${positions.size} token(s)`);
    for (const [addr, pos] of positions.entries()) {
      console.log(`    ${addr.substring(0, 10)}... qty=${pos.qtyOpen.toFixed(4)} | avg_cost=${pos.avgCostBase.toFixed(8)} ${pos.baseTokenSymbol}`);
    }
  }
  console.log('-'.repeat(60));

  // Gravar no banco
  console.log(`\n  Gravando ${updates.length} atualizações no banco...`);
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
