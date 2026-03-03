/**
 * Script de recálculo em massa de holding_time_s e is_long_trade
 * para todos os swap_events já gravados no MySQL.
 *
 * Uso:
 *   npm run recalculate-holding
 *
 * O que faz:
 *   1. Para cada swap_event com holding_time_s IS NULL:
 *      - Busca o swap de compra mais recente do mesmo token pela mesma carteira
 *      - Calcula holding_time_s = diferença em segundos
 *      - Define is_long_trade = 1 se >= threshold, 0 se < threshold
 *   2. Para todos os swap_events (inclusive os que já tinham holding_time_s):
 *      - Recalcula is_long_trade com o threshold atual do .env
 *   3. Recalcula kol_metrics (follow_score, scalping_rate, etc.) para todas as wallets
 *
 * Seguro para rodar múltiplas vezes (idempotente).
 */

import 'dotenv/config';
import { query, execute, closePool } from '../database/connection';
import { updateAllKolMetrics } from '../services/metrics-service';
import { config } from '../config';
import { logger } from '../utils/logger';

const BATCH_SIZE = 500; // registros por batch para não travar o MySQL
const SCALPING_THRESHOLD = config.indexer.scalpingThresholdSeconds;

async function recalculateHoldingTimes(): Promise<void> {
  logger.info('Starting holding time recalculation...', {
    scalping_threshold_s: SCALPING_THRESHOLD,
  });

  // ─────────────────────────────────────────────────────────────────────────
  // Passo 1: Recalcular holding_time_s para swaps que ainda não têm valor
  // ─────────────────────────────────────────────────────────────────────────
  const nullCount = await query<{ cnt: number }>(
    'SELECT COUNT(*) AS cnt FROM swap_events WHERE holding_time_s IS NULL'
  );
  const total = Number(nullCount[0]?.cnt) || 0;
  logger.info(`Found ${total} swap_events with holding_time_s = NULL to process`);

  let processed = 0;
  let updated = 0;

  // Processar em batches por wallet para aproveitar o índice
  const wallets = await query<{ wallet_address: string }>(
    `SELECT DISTINCT wallet_address
     FROM swap_events
     WHERE holding_time_s IS NULL
     ORDER BY wallet_address`
  );

  logger.info(`Processing ${wallets.length} wallets...`);

  for (const { wallet_address } of wallets) {
    // Buscar todos os swaps desta wallet ordenados por timestamp
    const swaps = await query<{
      id: number;
      tx_hash: string;
      timestamp: Date;
      token_in_address: string;
      token_out_address: string;
      holding_time_s: number | null;
    }>(
      `SELECT id, tx_hash, timestamp, token_in_address, token_out_address, holding_time_s
       FROM swap_events
       WHERE wallet_address = ?
       ORDER BY timestamp ASC`,
      [wallet_address]
    );

    // Mapa: token_address → timestamp da última compra (token_out desse swap)
    // Usamos isso para calcular holding sem fazer N queries por swap
    const lastBuyTime = new Map<string, number>();

    for (const swap of swaps) {
      processed++;
      const swapTime = new Date(swap.timestamp).getTime();

      // Calcular holding_time_s se ainda não existe
      if (swap.holding_time_s === null) {
        const buyTime = lastBuyTime.get(swap.token_in_address);
        let holdingTimeS: number | null = null;

        if (buyTime !== undefined) {
          const diff = Math.round((swapTime - buyTime) / 1000);
          // Sanidade: ignorar negativos e > 1 ano
          if (diff > 0 && diff <= 365 * 24 * 3600) {
            holdingTimeS = diff;
          }
        }

        const isLongTrade = holdingTimeS === null
          ? null
          : holdingTimeS >= SCALPING_THRESHOLD ? 1 : 0;

        await execute(
          'UPDATE swap_events SET holding_time_s = ?, is_long_trade = ? WHERE tx_hash = ? AND wallet_address = ?',
          [holdingTimeS, isLongTrade, swap.tx_hash, wallet_address]
        );
        updated++;
      }

      // Registrar esta compra para cálculo futuro dos próximos swaps
      // (token_out deste swap = token que o trader agora possui)
      lastBuyTime.set(swap.token_out_address, swapTime);
    }

    if (processed % 5000 === 0) {
      logger.info(`Progress: ${processed} swaps processed, ${updated} updated`);
    }
  }

  logger.info(`Holding time recalculation complete: ${updated} swaps updated`);

  // ─────────────────────────────────────────────────────────────────────────
  // Passo 2: Recalcular is_long_trade para TODOS os swaps com holding_time_s
  //          (garante que o threshold atual do .env seja aplicado)
  // ─────────────────────────────────────────────────────────────────────────
  logger.info(`Recalculating is_long_trade with threshold = ${SCALPING_THRESHOLD}s for all swaps...`);

  // Atualizar em lotes usando UPDATE direto no MySQL (muito mais rápido que row-by-row)
  const [longResult] = await query<{ affected: number }>(
    `UPDATE swap_events
     SET is_long_trade = CASE
       WHEN holding_time_s >= ${SCALPING_THRESHOLD} THEN 1
       ELSE 0
     END
     WHERE holding_time_s IS NOT NULL`
  );

  // Garantir NULL onde holding_time_s é NULL
  await query(
    `UPDATE swap_events SET is_long_trade = NULL WHERE holding_time_s IS NULL`
  );

  logger.info('is_long_trade recalculation complete');

  // ─────────────────────────────────────────────────────────────────────────
  // Passo 3: Recalcular kol_metrics para todas as wallets
  // ─────────────────────────────────────────────────────────────────────────
  logger.info('Recalculating kol_metrics (follow_score, scalping_rate, holding_avg) for all wallets...');
  await updateAllKolMetrics();
  logger.info('kol_metrics recalculation complete');
}

async function main(): Promise<void> {
  try {
    await recalculateHoldingTimes();
    logger.info('All recalculations completed successfully');
  } catch (error) {
    logger.error('Recalculation failed', { error: (error as Error).message });
    process.exit(1);
  } finally {
    await closePool();
    process.exit(0);
  }
}

main();
