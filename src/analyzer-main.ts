/**
 * analyzer-main.ts — KOLSCAN Analyzer
 *
 * Programa 2 de 2: responsável por:
 *   - Calcular follow score e todos os componentes (Followability, Consistência, PnL, Win Rate)
 *   - Atualizar kol_metrics para wallets ativas recentemente (incremental, a cada hora)
 *   - Detectar flags de comportamento suspeito: Scalper, Bundler, Sybil (diário 02:00 UTC)
 *   - Recalcular métricas de wallets ativas nas últimas 48h (diário 03:00 UTC)
 *
 * NÃO faz: indexação de blocos, conexão com RPC, gravação de swap_events.
 * Esses dados já devem estar no MySQL, populados pelo kolscan-indexer.
 *
 * Uso:
 *   npm run analyzer         → produção (usa dist/)
 *   npm run analyzer:dev     → desenvolvimento (ts-node)
 *
 * Pode rodar em paralelo com o indexer ou em horários distintos.
 * Recomendado: rodar continuamente em background para manter métricas atualizadas.
 */

import { testConnection, closePool } from './database/connection';
import { startAnalyzerJobs, stopAnalyzerJobs } from './jobs/analyzer-jobs';
import { config } from './config';
import { logger } from './utils/logger';

async function main(): Promise<void> {
  logger.info('═══════════════════════════════════════════════════════');
  logger.info('  KOLSCAN ANALYZER starting...', {
    env:  config.nodeEnv,
    mode: 'metrics & follow score calculation',
  });
  logger.info('═══════════════════════════════════════════════════════');

  // 1. Banco de dados
  logger.info('[1/2] Testing database connection...');
  const dbOk = await testConnection();
  if (!dbOk) {
    logger.error('Database connection failed. Exiting...');
    process.exit(1);
  }

  // 2. Jobs de análise
  logger.info('[2/2] Starting analyzer jobs...');
  startAnalyzerJobs();

  logger.info('═══════════════════════════════════════════════════════');
  logger.info('  KOLSCAN ANALYZER is running');
  logger.info('  Jobs ativos:');
  logger.info('    • Incremental metrics update  — a cada 1 hora');
  logger.info('    • Flags detection (Scalper, Bundler, Sybil) — 02:00 UTC diário');
  logger.info('    • Partial score recalculation — 03:00 UTC diário');
  logger.info('');
  logger.info('  Para recálculo COMPLETO de todas as wallets:');
  logger.info('    npm run recalculate-pnl   (pode levar horas)');
  logger.info('═══════════════════════════════════════════════════════');

  // Graceful shutdown
  const shutdown = async (signal: string): Promise<void> => {
    logger.info(`Received ${signal}, shutting down analyzer...`);
    stopAnalyzerJobs();
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

  // Manter o processo vivo
  await new Promise<void>(() => {});
}

main().catch((err: Error) => {
  logger.error('Fatal error during analyzer startup', { error: err.message, stack: err.stack });
  process.exit(1);
});
