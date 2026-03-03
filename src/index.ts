import { testConnection, closePool } from './database/connection';
import { runMigrations } from './database/migrations';
import { testProviderConnection, destroyProviders } from './indexer/provider';
import { startIndexer, stopIndexer } from './indexer/block-indexer';
import { startServer } from './api/server';
import { startMetricsUpdater, startPricesUpdater, stopAllJobs } from './jobs/metrics-updater';
import { config } from './config';
import { logger } from './utils/logger';

async function main(): Promise<void> {
  logger.info('Starting KOLScan Base...', {
    env: config.nodeEnv,
    chain: config.blockchain.chainName,
    chainId: config.blockchain.chainId,
  });

  // 1. Verificar conexão com banco de dados
  logger.info('Testing database connection...');
  const dbOk = await testConnection();
  if (!dbOk) {
    logger.error('Database connection failed. Exiting...');
    process.exit(1);
  }

  // 2. Executar migrations
  logger.info('Running database migrations...');
  await runMigrations();

  // 3. Verificar conexão com blockchain
  logger.info('Testing blockchain provider connection...');
  const providerOk = await testProviderConnection();
  if (!providerOk) {
    logger.warn('Blockchain provider connection failed. API will start but indexer will be disabled.');
  }

  // 4. Iniciar servidor da API
  logger.info('Starting API server...');
  await startServer();

  // 5. Iniciar jobs em background
  startMetricsUpdater();
  startPricesUpdater();

  // 6. Iniciar indexador (se habilitado e provider disponível)
  if (config.indexer.enabled && providerOk) {
    logger.info('Starting blockchain indexer...');
    startIndexer().catch((err) => {
      logger.error('Indexer crashed', { error: err.message });
    });
  } else {
    logger.info('Indexer disabled or provider unavailable. Skipping indexer start.');
  }

  // Graceful shutdown
  const shutdown = async (signal: string): Promise<void> => {
    logger.info(`Received ${signal}, shutting down...`);
    stopIndexer();
    stopAllJobs();
    destroyProviders();
    await closePool();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('uncaughtException', (err) => {
    logger.error('Uncaught exception', { error: err.message, stack: err.stack });
    process.exit(1);
  });
  process.on('unhandledRejection', (reason) => {
    logger.error('Unhandled rejection', { reason });
    process.exit(1);
  });
}

main().catch((err) => {
  logger.error('Fatal error during startup', { error: err.message, stack: err.stack });
  process.exit(1);
});
