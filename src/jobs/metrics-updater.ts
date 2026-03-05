/**
 * metrics-updater.ts
 *
 * Estratégia de atualização de métricas:
 *
 * 1. INCREMENTAL (automático, leve)
 *    Após cada swap gravado pelo indexador, updateKolMetrics(wallet) é chamado
 *    diretamente no block-indexer para atualizar apenas aquela wallet.
 *    Isso mantém o leaderboard atualizado em tempo real sem custo alto.
 *
 * 2. RECÁLCULO COMPLETO (manual, pesado)
 *    Deve ser executado via script dedicado:
 *      npm run recalculate-pnl       → recalcula PnL de todas as wallets
 *    O recálculo completo de métricas NÃO é feito automaticamente ao iniciar
 *    o servidor, pois com 36k+ wallets pode levar horas.
 *
 * 3. JOBS DIÁRIOS (automáticos, agendados)
 *    - 02:00 UTC: detecção de flags (Scalper, Bundler, Creator-Funded, Sybil)
 *    - 03:00 UTC: recálculo de scores para wallets ativas recentes (últimas 48h)
 */

import { updateKolMetrics, updateAllKolMetrics } from '../services/metrics-service';
import { runDailyFlagsUpdate } from '../services/flags-service';
import { updateAllTokenPrices } from '../services/price-service';
import { updateTokenMarketData } from '../services/token-service';
import { query } from '../database/connection';
import { config } from '../config';
import { logger } from '../utils/logger';

let pricesInterval: NodeJS.Timeout | null = null;
let marketDataInterval: NodeJS.Timeout | null = null;
let flagsTimeout: NodeJS.Timeout | null = null;
let scoresTimeout: NodeJS.Timeout | null = null;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers de agendamento diário
// ─────────────────────────────────────────────────────────────────────────────

function msUntilUtc(hour: number, minute: number): number {
  const now = new Date();
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
  logger.info(`${label} scheduled daily at ${hh}:${mm} UTC (first run in ${Math.round(delay / 60000)} min)`);

  return setTimeout(function tick() {
    fn().catch((err) => logger.error(`${label} failed`, { error: (err as Error).message }));
    setTimeout(tick, msUntilUtc(hour, minute));
  }, delay);
}

// ─────────────────────────────────────────────────────────────────────────────
// Recálculo parcial diário — apenas wallets ativas recentemente
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Recalcula métricas apenas para wallets que tiveram swaps nas últimas 48h.
 * Muito mais leve que o recálculo completo — roda em segundos/minutos.
 */
async function recalculateRecentWallets(): Promise<void> {
  const rows = await query<{ wallet_address: string }>(
    `SELECT DISTINCT wallet_address
     FROM swap_events
     WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 48 HOUR)`
  );

  if (rows.length === 0) {
    logger.info('No recent wallets to recalculate');
    return;
  }

  logger.info(`Recalculating metrics for ${rows.length} recently active wallets...`);
  let done = 0;
  for (const row of rows) {
    try {
      await updateKolMetrics(row.wallet_address);
    } catch (err) {
      logger.warn(`Failed to update metrics for ${row.wallet_address}`, {
        error: (err as Error).message,
      });
    }
    done++;
    if (done % 100 === 0) {
      logger.info(`  ${done}/${rows.length} wallets recalculated...`);
    }
  }
  logger.info(`Recalculation of recent wallets completed (${rows.length} wallets)`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Jobs
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Inicia os jobs em background.
 *
 * NÃO faz recálculo completo ao iniciar — isso deve ser feito manualmente
 * via `npm run recalculate-pnl` quando necessário.
 *
 * Jobs agendados:
 * - 02:00 UTC: flags (Scalper, Bundler, Creator-Funded, Sybil)
 * - 03:00 UTC: recálculo de scores para wallets ativas nas últimas 48h
 */
export async function startMetricsUpdater(): Promise<void> {
  logger.info('Starting metrics updater job...');
  logger.info('Note: full recalculation must be run manually via "npm run recalculate-pnl"');
  logger.info('      Incremental updates happen automatically per swap in the indexer.');

  // ── Job de flags — 02:00 UTC ──────────────────────────────────────────────
  flagsTimeout = scheduleDailyUtc(2, 0, 'Flags job (02:00 UTC)', async () => {
    logger.info('Running daily flags detection...');
    await runDailyFlagsUpdate();
    logger.info('Daily flags detection completed');
  });

  // ── Job de scores — 03:00 UTC ─────────────────────────────────────────────
  // Recalcula apenas wallets ativas nas últimas 48h (leve, não o recálculo completo)
  scoresTimeout = scheduleDailyUtc(3, 0, 'Partial score recalculation (03:00 UTC)', async () => {
    logger.info('Running daily partial score recalculation (recent wallets only)...');
    await recalculateRecentWallets();
    logger.info('Daily partial score recalculation completed');
  });
}

/**
 * Inicia o job de atualização de preços de tokens (a cada 2 min)
 */
export function startPricesUpdater(): void {
  logger.info('Starting prices updater job...');

  pricesInterval = setInterval(async () => {
    try {
      await updateAllTokenPrices();
    } catch (error) {
      logger.error('Prices update job failed', { error: (error as Error).message });
    }
  }, 2 * 60 * 1000);

  logger.info('Prices updater scheduled every 2 minutes');
}

/**
 * Inicia o job de atualização de market data dos tokens ativos (a cada 5 min)
 */
export function startTokenMarketDataUpdater(): void {
  logger.info('Starting token market data updater job...');

  updateTokenMarketData().catch((err) =>
    logger.error('Initial token market data update failed', { error: (err as Error).message })
  );

  marketDataInterval = setInterval(async () => {
    try {
      await updateTokenMarketData();
    } catch (error) {
      logger.error('Token market data update job failed', { error: (error as Error).message });
    }
  }, 5 * 60 * 1000);

  logger.info('Token market data updater scheduled every 5 minutes');
}

/**
 * Para todos os jobs em background
 */
export function stopAllJobs(): void {
  if (pricesInterval)     { clearInterval(pricesInterval);     pricesInterval     = null; }
  if (marketDataInterval) { clearInterval(marketDataInterval); marketDataInterval = null; }
  if (flagsTimeout)       { clearTimeout(flagsTimeout);        flagsTimeout       = null; }
  if (scoresTimeout)      { clearTimeout(scoresTimeout);       scoresTimeout      = null; }
  logger.info('All background jobs stopped');
}
