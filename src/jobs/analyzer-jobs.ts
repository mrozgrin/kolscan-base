/**
 * analyzer-jobs.ts — Jobs do KOLSCAN Analyzer
 *
 * Jobs de análise que rodam no processo separado (kolscan-analyzer):
 *
 *   1. INCREMENTAL — a cada 1 hora
 *      Recalcula métricas apenas para wallets com swaps nas últimas 2h.
 *      Muito leve — mantém o leaderboard atualizado sem custo alto.
 *
 *   2. FLAGS — 02:00 UTC diário
 *      Detecta comportamentos suspeitos: Scalper, Bundler, Creator-Funded, Sybil.
 *
 *   3. SCORES PARCIAL — 03:00 UTC diário
 *      Recalcula follow score para wallets ativas nas últimas 48h.
 *
 * Para recálculo COMPLETO de todas as wallets:
 *   npm run recalculate-pnl   (manual, pode levar horas)
 */

import { updateKolMetrics } from '../services/metrics-service';
import { runDailyFlagsUpdate } from '../services/flags-service';
import { query } from '../database/connection';
import { logger } from '../utils/logger';

let incrementalInterval: NodeJS.Timeout | null = null;
let flagsTimeout:        NodeJS.Timeout | null = null;
let scoresTimeout:       NodeJS.Timeout | null = null;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers de agendamento diário
// ─────────────────────────────────────────────────────────────────────────────

function msUntilUtc(hour: number, minute: number): number {
  const now  = new Date();
  const next = new Date();
  next.setUTCHours(hour, minute, 0, 0);
  if (next.getTime() <= now.getTime()) {
    next.setUTCDate(next.getUTCDate() + 1);
  }
  return next.getTime() - now.getTime();
}

function scheduleDailyUtc(
  hour: number,
  minute: number,
  label: string,
  fn: () => Promise<void>
): NodeJS.Timeout {
  const delay = msUntilUtc(hour, minute);
  const hh = String(hour).padStart(2, '0');
  const mm = String(minute).padStart(2, '0');
  const mins = Math.round(delay / 60000);
  logger.info(`[analyzer-jobs] ${label} → próxima execução: ${hh}:${mm} UTC (em ${mins} min)`);

  return setTimeout(function tick() {
    fn()
      .then(() => logger.info(`[analyzer-jobs] ${label} concluído`))
      .catch((err) => logger.error(`[analyzer-jobs] ${label} falhou`, { error: (err as Error).message }));
    setTimeout(tick, msUntilUtc(hour, minute));
  }, delay);
}

// ─────────────────────────────────────────────────────────────────────────────
// Job incremental — wallets ativas nas últimas 2h
// ─────────────────────────────────────────────────────────────────────────────

async function runIncrementalUpdate(): Promise<void> {
  const rows = await query<{ wallet_address: string }>(
    `SELECT DISTINCT wallet_address
     FROM swap_events
     WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 2 HOUR)`
  );

  if (rows.length === 0) {
    logger.info('[analyzer-jobs] Incremental update: no new swaps in the last 2h');
    return;
  }

  logger.info(`[analyzer-jobs] Incremental update: ${rows.length} wallet(s) with recent swaps...`);
  let done = 0;
  for (const row of rows) {
    try {
      await updateKolMetrics(row.wallet_address);
    } catch (err) {
      logger.warn(`[analyzer-jobs] Failed to update metrics for ${row.wallet_address}`, {
        error: (err as Error).message,
      });
    }
    done++;
    if (done % 50 === 0) {
      logger.info(`[analyzer-jobs]   ${done}/${rows.length} wallets updated...`);
    }
  }
  logger.info(`[analyzer-jobs] Incremental update completed (${rows.length} wallets)`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Job parcial diário — wallets ativas nas últimas 48h
// ─────────────────────────────────────────────────────────────────────────────

async function runPartialScoreRecalculation(): Promise<void> {
  const rows = await query<{ wallet_address: string }>(
    `SELECT DISTINCT wallet_address
     FROM swap_events
     WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 48 HOUR)`
  );

  if (rows.length === 0) {
    logger.info('[analyzer-jobs] Partial recalculation: no active wallets in the last 48h');
    return;
  }

  logger.info(`[analyzer-jobs] Partial recalculation: ${rows.length} wallet(s)...`);
  let done = 0;
  for (const row of rows) {
    try {
      await updateKolMetrics(row.wallet_address);
    } catch (err) {
      logger.warn(`[analyzer-jobs] Failed to recalculate ${row.wallet_address}`, {
        error: (err as Error).message,
      });
    }
    done++;
    if (done % 100 === 0) {
      logger.info(`[analyzer-jobs]   ${done}/${rows.length} wallets recalculated...`);
    }
  }
  logger.info(`[analyzer-jobs] Partial recalculation completed (${rows.length} wallets)`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Iniciar / parar
// ─────────────────────────────────────────────────────────────────────────────

export function startAnalyzerJobs(): void {
  logger.info('[analyzer-jobs] Starting all analyzer jobs...');

  // 1. Incremental — a cada 1 hora
  logger.info('[analyzer-jobs] Incremental metrics update → every 1 hour');
  runIncrementalUpdate().catch((err) =>
    logger.error('[analyzer-jobs] Initial incremental update failed', { error: (err as Error).message })
  );
  incrementalInterval = setInterval(() => {
    runIncrementalUpdate().catch((err) =>
      logger.error('[analyzer-jobs] Incremental update failed', { error: (err as Error).message })
    );
  }, 60 * 60 * 1000); // 1 hora

  // 2. Flags — 02:00 UTC
  flagsTimeout = scheduleDailyUtc(2, 0, 'Flags detection (Scalper/Bundler/Sybil)', async () => {
    logger.info('[analyzer-jobs] Running daily flags detection...');
    await runDailyFlagsUpdate();
  });

  // 3. Scores parcial — 03:00 UTC
  scoresTimeout = scheduleDailyUtc(3, 0, 'Partial score recalculation (wallets 48h)', async () => {
    logger.info('[analyzer-jobs] Running daily partial score recalculation...');
    await runPartialScoreRecalculation();
  });

  logger.info('[analyzer-jobs] All analyzer jobs started');
}

export function stopAnalyzerJobs(): void {
  if (incrementalInterval) { clearInterval(incrementalInterval); incrementalInterval = null; }
  if (flagsTimeout)        { clearTimeout(flagsTimeout);         flagsTimeout        = null; }
  if (scoresTimeout)       { clearTimeout(scoresTimeout);        scoresTimeout       = null; }
  logger.info('[analyzer-jobs] All analyzer jobs stopped');
}
