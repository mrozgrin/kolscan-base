import { updateAllKolMetrics } from '../services/metrics-service';
import { updateAllTokenPrices } from '../services/price-service';
import { config } from '../config';
import { logger } from '../utils/logger';

let metricsInterval: NodeJS.Timeout | null = null;
let pricesInterval: NodeJS.Timeout | null = null;

/**
 * Inicia o job de atualização de métricas dos KOLs.
 *
 * Ao reiniciar o programa, o recálculo é executado IMEDIATAMENTE e de forma
 * síncrona (await) antes de retornar, garantindo que o leaderboard e o
 * follow_score já estejam atualizados com os dados do MySQL quando a API
 * começar a receber requisições.
 *
 * Após o recálculo inicial, o job continua rodando periodicamente conforme
 * METRICS_UPDATE_INTERVAL (padrão: 5 minutos).
 */
export async function startMetricsUpdater(): Promise<void> {
  logger.info('Starting metrics updater job...');

  // Recalcular AGORA com os dados já gravados no MySQL.
  // Isso garante que follow_score, profit_pct e holding_time estejam
  // corretos mesmo após um restart sem reindexar a blockchain.
  logger.info('Running initial metrics recalculation from existing MySQL data...');
  try {
    await updateAllKolMetrics();
    logger.info('Initial metrics recalculation completed — leaderboard is ready');
  } catch (err) {
    logger.error('Initial metrics recalculation failed', { error: (err as Error).message });
  }

  // Agendar atualizações periódicas
  metricsInterval = setInterval(async () => {
    try {
      await updateAllKolMetrics();
    } catch (error) {
      logger.error('Metrics update job failed', { error: (error as Error).message });
    }
  }, config.indexer.metricsUpdateInterval);

  logger.info(`Metrics updater scheduled every ${config.indexer.metricsUpdateInterval / 1000}s`);
}

/**
 * Inicia o job de atualização de preços de tokens
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
 * Para todos os jobs em background
 */
export function stopAllJobs(): void {
  if (metricsInterval) {
    clearInterval(metricsInterval);
    metricsInterval = null;
  }

  if (pricesInterval) {
    clearInterval(pricesInterval);
    pricesInterval = null;
  }

  logger.info('All background jobs stopped');
}
