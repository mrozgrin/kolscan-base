import { updateAllKolMetrics } from '../services/metrics-service';
import { runDailyFlagsUpdate } from '../services/flags-service';
import { updateAllTokenPrices } from '../services/price-service';
import { updateTokenMarketData } from '../services/token-service';
import { config } from '../config';
import { logger } from '../utils/logger';

let metricsInterval: NodeJS.Timeout | null = null;
let pricesInterval: NodeJS.Timeout | null = null;
let marketDataInterval: NodeJS.Timeout | null = null;
let flagsTimeout: NodeJS.Timeout | null = null;
let scoresTimeout: NodeJS.Timeout | null = null;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers de agendamento diário
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Retorna quantos ms faltam para o próximo HH:MM UTC.
 */
function msUntilUtc(hour: number, minute: number): number {
  const now = new Date();
  const next = new Date();
  next.setUTCHours(hour, minute, 0, 0);
  if (next.getTime() <= now.getTime()) {
    next.setUTCDate(next.getUTCDate() + 1);
  }
  return next.getTime() - now.getTime();
}

/**
 * Agenda uma função para rodar todo dia em HH:MM UTC, repetindo a cada 24h.
 */
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
    // Re-agendar para o mesmo horário no dia seguinte
    setTimeout(tick, msUntilUtc(hour, minute));
  }, delay);
}

// ─────────────────────────────────────────────────────────────────────────────
// Jobs
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Inicia o job de atualização de métricas dos KOLs.
 *
 * - Recalcula IMEDIATAMENTE ao iniciar (garante leaderboard atualizado após restart)
 * - Roda a cada METRICS_UPDATE_INTERVAL (padrão: 5 min) durante o dia
 * - Roda o job de flags às 02:00 UTC (Scalper, Bundler, Creator-Funded, Sybil)
 * - Roda o recálculo completo de scores às 03:00 UTC
 */
export async function startMetricsUpdater(): Promise<void> {
  logger.info('Starting metrics updater job...');

  // Recálculo inicial síncrono — garante que o leaderboard esteja pronto
  // antes da API começar a aceitar requisições.
  logger.info('Running initial metrics recalculation from existing MySQL data...');
  try {
    await updateAllKolMetrics();
    logger.info('Initial metrics recalculation completed — leaderboard is ready');
  } catch (err) {
    logger.error('Initial metrics recalculation failed', { error: (err as Error).message });
  }

  // Atualização periódica intraday (a cada 5 min por padrão)
  metricsInterval = setInterval(async () => {
    try {
      await updateAllKolMetrics();
    } catch (error) {
      logger.error('Metrics update job failed', { error: (error as Error).message });
    }
  }, config.indexer.metricsUpdateInterval);

  logger.info(`Metrics updater scheduled every ${config.indexer.metricsUpdateInterval / 1000}s`);

  // ── Job de flags — 02:00 UTC ──────────────────────────────────────────────
  // Detecta Scalper, Bundler, Creator-Funded e Sybil.
  // Wallets desqualificadas são removidas do leaderboard automaticamente.
  flagsTimeout = scheduleDailyUtc(2, 0, 'Flags job (02:00 UTC)', async () => {
    logger.info('Running daily flags detection...');
    await runDailyFlagsUpdate();
    logger.info('Daily flags detection completed');
  });

  // ── Job de scores — 03:00 UTC ─────────────────────────────────────────────
  // Recalcula o Follow Score completo (4 componentes) para todas as wallets.
  // Roda após o job de flags para garantir que wallets desqualificadas
  // já foram marcadas antes do recálculo.
  scoresTimeout = scheduleDailyUtc(3, 0, 'Full score recalculation (03:00 UTC)', async () => {
    logger.info('Running daily full score recalculation...');
    await updateAllKolMetrics();
    logger.info('Daily full score recalculation completed');
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
 * Atualiza preço, price_change_24h, liquidity, volume para os 100 tokens mais ativos
 */
export function startTokenMarketDataUpdater(): void {
  logger.info('Starting token market data updater job...');

  // Rodar imediatamente na inicialização
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
  if (metricsInterval)    { clearInterval(metricsInterval);    metricsInterval    = null; }
  if (pricesInterval)     { clearInterval(pricesInterval);     pricesInterval     = null; }
  if (marketDataInterval) { clearInterval(marketDataInterval); marketDataInterval = null; }
  if (flagsTimeout)       { clearTimeout(flagsTimeout);        flagsTimeout       = null; }
  if (scoresTimeout)      { clearTimeout(scoresTimeout);       scoresTimeout      = null; }
  logger.info('All background jobs stopped');
}
