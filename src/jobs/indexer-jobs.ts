/**
 * indexer-jobs.ts — Jobs do KOLSCAN Indexer
 *
 * Jobs leves que rodam junto com o indexador:
 *   - Atualização de preços de tokens (a cada 2 min)
 *   - Atualização de market data dos tokens ativos (a cada 5 min)
 *
 * NÃO inclui: cálculo de follow score, flags, métricas agregadas.
 * Esses jobs ficam no analyzer-jobs.ts (kolscan-analyzer).
 */

import { updateAllTokenPrices } from '../services/price-service';
import { updateTokenMarketData } from '../services/token-service';
import { logger } from '../utils/logger';

let pricesInterval:     NodeJS.Timeout | null = null;
let marketDataInterval: NodeJS.Timeout | null = null;

/**
 * Inicia o job de atualização de preços de tokens (a cada 2 min)
 */
export function startPricesUpdater(): void {
  logger.info('[indexer-jobs] Starting prices updater (every 2 min)...');

  // Primeira execução imediata
  updateAllTokenPrices().catch((err) =>
    logger.error('[indexer-jobs] Initial prices update failed', { error: (err as Error).message })
  );

  pricesInterval = setInterval(async () => {
    try {
      await updateAllTokenPrices();
    } catch (err) {
      logger.error('[indexer-jobs] Prices update failed', { error: (err as Error).message });
    }
  }, 2 * 60 * 1000);
}

/**
 * Inicia o job de atualização de market data dos tokens ativos (a cada 5 min)
 */
export function startTokenMarketDataUpdater(): void {
  logger.info('[indexer-jobs] Starting token market data updater (every 5 min)...');

  updateTokenMarketData().catch((err) =>
    logger.error('[indexer-jobs] Initial market data update failed', { error: (err as Error).message })
  );

  marketDataInterval = setInterval(async () => {
    try {
      await updateTokenMarketData();
    } catch (err) {
      logger.error('[indexer-jobs] Market data update failed', { error: (err as Error).message });
    }
  }, 5 * 60 * 1000);
}

/**
 * Para todos os jobs do indexer
 */
export function stopAllJobs(): void {
  if (pricesInterval)     { clearInterval(pricesInterval);     pricesInterval     = null; }
  if (marketDataInterval) { clearInterval(marketDataInterval); marketDataInterval = null; }
  logger.info('[indexer-jobs] All indexer jobs stopped');
}
