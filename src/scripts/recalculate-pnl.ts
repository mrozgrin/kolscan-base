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

// Número de wallets processadas em paralelo (não aumentar muito para não sobrecarregar o MySQL)
const WALLET_CONCURRENCY = 5;

// ─────────────────────────────────────────────────────────────────────────────
// Tipos internos
// ─────────────────────────────────────────────────────────────────────────────

interface SwapRow {
  id: number;
  tx_hash: string;
  timestamp: Date;
  token_in_address: string;
  token_in_symbol: string;
  token_in_amount: string;   // raw (não dividido por decimais — já está dividido no banco)
  token_out_address: string;
  token_out_symbol: string;
  token_out_amount: string;  // raw
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

/**
 * Reprocessa todos os swaps de uma wallet em ordem cronológica,
 * reconstruindo as posições e calculando o PnL real na moeda base.
 *
 * Retorna as posições ainda abertas ao final (para gravar na tabela positions).
 */
async function reprocessWallet(
  walletAddress: string
): Promise<Map<string, Position>> {
  // Buscar todos os swaps desta wallet em ordem cronológica
  // NOTA: token_in_amount e token_out_amount já estão em unidades humanas no banco
  //       (o block-indexer divide por 10^decimals antes de gravar em value_usd,
  //        mas grava os amounts RAW em token_in_amount/token_out_amount).
  //       Por isso precisamos dos decimais para converter.
  const swaps = await query<SwapRow & { token_in_decimals: number | null; token_out_decimals: number | null }>(
    `SELECT
       se.id, se.tx_hash, se.timestamp,
       se.token_in_address,  se.token_in_symbol,  se.token_in_amount,
       se.token_out_address, se.token_out_symbol, se.token_out_amount,
       ti.decimals AS token_in_decimals,
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

  // Batch de UPDATEs para executar no final (evita N queries individuais)
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

    // Converter amounts de raw (wei) para unidades humanas
    const inDecimals  = swap.token_in_decimals  ?? 18;
    const outDecimals = swap.token_out_decimals ?? 18;
    const amtIn  = parseFloat(swap.token_in_amount)  / Math.pow(10, inDecimals);
    const amtOut = parseFloat(swap.token_out_amount) / Math.pow(10, outDecimals);

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
        // Compra adicional: atualiza custo médio ponderado
        const newQty  = pos.qtyOpen       + amtOut;
        const newCost = pos.costBasisBase + amtIn;
        pos.qtyOpen       = newQty;
        pos.costBasisBase = newCost;
        pos.avgCostBase   = newQty > 0 ? newCost / newQty : 0;
        // Mantém a moeda base original da posição
      }

    } else if (swapType === 'sell') {
      // ── VENDA: fecha ou reduz posição e calcula PnL ───────────────────────
      const baseAddr   = outAddr;
      const baseSymbol = BASE_TOKENS.get(baseAddr) ?? swap.token_out_symbol;

      const pos = positions.get(inAddr);

      if (pos && pos.qtyOpen > 0) {
        // PnL = valor recebido em base − custo proporcional
        const costProporcional = pos.avgCostBase * amtIn;
        pnlBase       = amtOut - costProporcional;
        pnlBaseToken  = baseAddr;
        pnlBaseSymbol = baseSymbol;
        isWin         = pnlBase > 0 ? 1 : 0;

        // Reduzir posição proporcionalmente
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
        // Registra swap_type mas não calcula PnL
        pnlBase       = null;
        pnlBaseToken  = baseAddr;
        pnlBaseSymbol = baseSymbol;
        isWin         = null;
      }

    } else {
      // ── SWAP meme→meme: fecha posição do token vendido sem PnL em base ───
      const pos = positions.get(inAddr);
      if (pos && pos.qtyOpen > 0) {
        // Fecha posição do token vendido
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

      // Abre posição do token recebido usando o custo da posição anterior como proxy
      // (sem moeda base real — usamos o token vendido como referência)
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

    updates.push({
      id: swap.id,
      swapType,
      pnlBase,
      pnlBaseToken,
      pnlBaseSymbol,
      isWin,
    });
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

  // Contar total de swaps para progresso
  const totalSwapsResult = await query<{ cnt: number }>(
    'SELECT COUNT(*) AS cnt FROM swap_events'
  );
  const totalSwaps = Number(totalSwapsResult[0]?.cnt) || 0;
  logger.info(`Total de swap_events no banco: ${totalSwaps}`);

  // Buscar todas as wallets com swaps
  const wallets = await query<{ wallet_address: string; cnt: number }>(
    `SELECT wallet_address, COUNT(*) AS cnt
     FROM swap_events
     GROUP BY wallet_address
     ORDER BY cnt DESC`
  );
  logger.info(`Total de wallets a processar: ${wallets.length}`);

  // ── Passo 1: Limpar tabela positions (será reconstruída) ──────────────────
  logger.info('Limpando tabela positions...');
  await execute('DELETE FROM positions', []);
  logger.info('Tabela positions limpa.');

  // ── Passo 2: Reprocessar cada wallet ─────────────────────────────────────
  let walletsProcessed = 0;
  let swapsProcessed   = 0;

  // Processar em lotes de WALLET_CONCURRENCY wallets em paralelo
  for (let i = 0; i < wallets.length; i += WALLET_CONCURRENCY) {
    const batch = wallets.slice(i, i + WALLET_CONCURRENCY);

    const results = await Promise.all(
      batch.map(async ({ wallet_address, cnt }) => {
        const openPositions = await reprocessWallet(wallet_address);
        return { wallet_address, cnt, openPositions };
      })
    );

    // Gravar posições abertas restantes na tabela positions
    for (const { wallet_address, cnt, openPositions } of results) {
      for (const [tokenAddress, pos] of openPositions.entries()) {
        if (pos.qtyOpen <= 0) continue;
        await execute(
          `INSERT INTO positions
             (wallet_address, token_address, base_token_address, base_token_symbol,
              qty_open, cost_basis_base, avg_cost_base, opened_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             qty_open        = VALUES(qty_open),
             cost_basis_base = VALUES(cost_basis_base),
             avg_cost_base   = VALUES(avg_cost_base),
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

  // ── Passo 3: Recalcular kol_metrics para todas as wallets ─────────────────
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
