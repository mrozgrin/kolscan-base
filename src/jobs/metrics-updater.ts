import { updateAllKolMetrics } from '../services/metrics-service';
import { updateAllTokenPrices } from '../services/price-service';
import { config } from '../config';
import { logger } from '../utils/logger';

let metricsInterval: NodeJS.Timeout | null = null;
let pricesInterval: NodeJS.Timeout | null = null;

/**
 * Inicia o job de atualização de métricas dos KOLs
 */
export function startMetricsUpdater(): void {
  logger.info('Starting metrics updater job...');

  // Executar imediatamente na inicialização
  updateAllKolMetrics().catch((err) => {
    logger.error('Initial metrics update failed', { error: err.message });
  });

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

  // Atualizar preços a cada 2 minutos
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
 * Para todos os jobs
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
