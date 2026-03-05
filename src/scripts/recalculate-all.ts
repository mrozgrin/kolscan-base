/**
 * recalculate-all.ts
 *
 * Executa o recálculo completo em sequência:
 *   1. recalculate-pnl  → reconstrói posições e calcula pnl_base / pnl_pct para todos os swap_events
 *   2. updateAllKolMetrics → recalcula follow score e todos os indicadores para todas as wallets
 *
 * Uso:
 *   npm run recalculate-all
 *
 * ATENÇÃO: Este script pode levar várias horas para bases grandes.
 * Para testar antes, use: npm run recalculate-pnl-amostra
 */

import { execSync } from 'child_process';
import { logger } from '../utils/logger';

async function main() {
  const start = Date.now();

  logger.info('=== recalculate-all: iniciando ===');
  logger.info('Passo 1/2: Recalculando PnL (posições e pnl_base)...');

  try {
    execSync('npx ts-node src/scripts/recalculate-pnl.ts', {
      stdio: 'inherit',
      cwd: process.cwd(),
    });
  } catch (err) {
    logger.error('Erro no recalculate-pnl', { error: (err as Error).message });
    process.exit(1);
  }

  logger.info('Passo 1/2 concluído. Iniciando Passo 2/2: Recalculando métricas e follow scores...');

  try {
    const { updateAllKolMetrics } = await import('../services/metrics-service');
    await updateAllKolMetrics();
  } catch (err) {
    logger.error('Erro no updateAllKolMetrics', { error: (err as Error).message });
    process.exit(1);
  }

  const elapsed = Math.round((Date.now() - start) / 1000);
  const h = Math.floor(elapsed / 3600);
  const m = Math.floor((elapsed % 3600) / 60);
  const s = elapsed % 60;
  logger.info(`=== recalculate-all: concluído em ${h}h ${m}m ${s}s ===`);
  process.exit(0);
}

main().catch((err) => {
  logger.error('recalculate-all error', { error: (err as Error).message });
  process.exit(1);
});
