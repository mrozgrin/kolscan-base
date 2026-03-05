/**
 * indexer-main.ts — KOLSCAN Indexer
 *
 * Programa 1 de 2: responsável por:
 *   - Conectar à blockchain Base via RPC
 *   - Indexar blocos e extrair eventos de swap
 *   - Calcular PnL por posição (compra→venda) em tempo real
 *   - Gravar swap_events, wallets, transactions e positions no MySQL
 *   - Atualizar preços de tokens (a cada 2 min)
 *   - Atualizar market data dos tokens ativos (a cada 5 min)
 *   - Servir a API REST na porta configurada
 *
 * NÃO faz: cálculo de follow score, métricas agregadas, flags de comportamento.
 * Esses cálculos são responsabilidade do kolscan-analyzer (analyzer-main.ts).
 *
 * Uso:
 *   npm run indexer          → produção (usa dist/)
 *   npm run indexer:dev      → desenvolvimento (ts-node, hot-reload)
 *   npm run indexer:api-only → sobe só a API, sem indexar blocos
 */

import { testConnection, closePool } from './database/connection';
import { runMigrations } from './database/migrations';
import { testProviderConnection, destroyProviders } from './indexer/provider';
import { startIndexer, stopIndexer } from './indexer/block-indexer';
import { startServer } from './api/server';
import { startPricesUpdater, startTokenMarketDataUpdater, stopAllJobs } from './jobs/indexer-jobs';
import { config } from './config';
import { logger } from './utils/logger';

async function main(): Promise<void> {
  logger.info('═══════════════════════════════════════════════════════');
  logger.info('  KOLSCAN INDEXER starting...', {
    env:     config.nodeEnv,
    chain:   config.blockchain.chainName,
    chainId: config.blockchain.chainId,
  });
  logger.info('═══════════════════════════════════════════════════════');

  // 1. Banco de dados
  logger.info('[1/5] Testing database connection...');
  const dbOk = await testConnection();
  if (!dbOk) {
    logger.error('Database connection failed. Exiting...');
    process.exit(1);
  }

  // 2. Migrations
  logger.info('[2/5] Running database migrations...');
  await runMigrations();

  // 3. Blockchain provider
  logger.info('[3/5] Testing blockchain provider connection...');
  const providerOk = await testProviderConnection();
  if (!providerOk) {
    logger.warn('Blockchain provider connection failed. API will start but indexer will be disabled.');
  }

  // 4. API REST
  logger.info('[4/5] Starting API server...');
  await startServer();

  // 5. Jobs de preços (leves, não bloqueantes)
  logger.info('[5/5] Starting background jobs (prices, market data)...');
  startPricesUpdater();
  startTokenMarketDataUpdater();

  // 6. Indexador de blocos
  if (config.indexer.enabled && providerOk) {
    logger.info('Starting blockchain indexer...');
    startIndexer().catch((err: Error) => {
      logger.error('Indexer crashed', { error: err.message });
    });
  } else {
    logger.info('Indexer disabled or provider unavailable. Running in API-only mode.');
  }

  logger.info('═══════════════════════════════════════════════════════');
  logger.info('  KOLSCAN INDEXER is running');
  logger.info(`  API: http://localhost:${config.api.port}`);
  logger.info('  Analyzer: run "npm run analyzer" in a separate terminal');
  logger.info('═══════════════════════════════════════════════════════');

  // Graceful shutdown
  const shutdown = async (signal: string): Promise<void> => {
    logger.info(`Received ${signal}, shutting down indexer...`);
    stopIndexer();
    stopAllJobs();
    destroyProviders();
    await closePool();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
  process.on('uncaughtException', (err) => {
    logger.error('Uncaught exception', { error: err.message, stack: err.stack });
    process.exit(1);
  });
  process.on('unhandledRejection', (reason) => {
    logger.error('Unhandled rejection', { reason });
    process.exit(1);
  });
}

main().catch((err: Error) => {
  logger.error('Fatal error during indexer startup', { error: err.message, stack: err.stack });
  process.exit(1);
});
