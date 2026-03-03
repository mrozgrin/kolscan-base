/**
 * flags-service.ts
 *
 * Serviço de detecção e atualização das flags de desqualificação de KOLs.
 * Executado diariamente às 02:00 UTC pelo jobs/flags-updater.ts.
 *
 * Flags implementadas:
 *   FLAG_SCALPER       — taxa de copiabilidade < 20% (holding < 120s na maioria dos trades)
 *   FLAG_BUNDLER       — > 5% dos trades no mesmo bloco (holding <= 4s)
 *   FLAG_CREATOR_FUNDED — recebeu fundos do deployer do token antes da primeira compra
 *   FLAG_SYBIL         — score de coordenação >= 3 pontos entre wallets
 *
 * is_disqualified = 1 se qualquer flag estiver ativa.
 */

import { query, execute, ddl } from '../database/connection';
import { logger } from '../utils/logger';
import { config } from '../config';

// ─────────────────────────────────────────────────────────────────────────────
// FLAG_SCALPER
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Detecta e atualiza a flag de scalper para todas as wallets ativas.
 * Scalper: taxa de copiabilidade < 20% (menos de 20% dos trades com holding >= threshold).
 */
export async function updateScalperFlags(): Promise<{ updated: number; scalpers: number }> {
  const threshold = config.indexer.scalpingThresholdSeconds;
  const minTrades = config.indexer.minTradesForKol;

  logger.info('Updating FLAG_SCALPER...', { threshold, minTrades });

  // Atualizar wallets com dados suficientes
  await ddl(`
    UPDATE wallets w
    JOIN (
      SELECT
        wallet_address,
        COUNT(*)                                                                   AS total_trades,
        SUM(CASE WHEN holding_time_s >= ${threshold} THEN 1 ELSE 0 END)           AS copiable_trades,
        AVG(holding_time_s)                                                        AS avg_holding_time,
        SUM(CASE WHEN holding_time_s >= ${threshold} THEN 1 ELSE 0 END) / COUNT(*) AS copiability_index
      FROM swap_events
      WHERE holding_time_s IS NOT NULL
        AND timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
      GROUP BY wallet_address
      HAVING COUNT(*) >= ${minTrades}
    ) metrics ON w.address = metrics.wallet_address
    SET
      w.flag_scalper              = IF(metrics.copiability_index < 0.20, 1, 0),
      w.scalper_copiability_index = metrics.copiability_index,
      w.scalper_copiable_trades   = metrics.copiable_trades,
      w.scalper_total_trades      = metrics.total_trades,
      w.scalper_avg_holding_time  = metrics.avg_holding_time,
      w.scalper_checked_at        = NOW()
  `);

  // Limpar flag de wallets sem dados suficientes
  await ddl(`
    UPDATE wallets
    SET flag_scalper = 0, scalper_checked_at = NOW()
    WHERE address NOT IN (
      SELECT wallet_address FROM swap_events
      WHERE holding_time_s IS NOT NULL
        AND timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
      GROUP BY wallet_address
      HAVING COUNT(*) >= ${minTrades}
    )
    AND flag_scalper = 1
  `);

  const stats = await query<{ updated: number; scalpers: number }>(
    `SELECT
       COUNT(*) AS updated,
       SUM(CASE WHEN flag_scalper = 1 THEN 1 ELSE 0 END) AS scalpers
     FROM wallets
     WHERE scalper_checked_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)`
  );

  const result = { updated: Number(stats[0]?.updated) || 0, scalpers: Number(stats[0]?.scalpers) || 0 };
  logger.info('FLAG_SCALPER updated', result);
  return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// FLAG_BUNDLER
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Detecta bundlers: wallets com > 5% dos trades com holding <= 4s (mesmo bloco).
 */
export async function updateBundlerFlags(): Promise<{ updated: number; bundlers: number }> {
  const minTrades = config.indexer.minTradesForKol;

  logger.info('Updating FLAG_BUNDLER...');

  await ddl(`
    UPDATE wallets w
    JOIN (
      SELECT
        wallet_address,
        COUNT(*)                                                             AS total_trades,
        SUM(CASE WHEN holding_time_s <= 4 THEN 1 ELSE 0 END)               AS same_block_trades,
        SUM(CASE WHEN holding_time_s <= 4 THEN 1 ELSE 0 END) / COUNT(*)    AS same_block_pct
      FROM swap_events
      WHERE holding_time_s IS NOT NULL
        AND timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
      GROUP BY wallet_address
      HAVING COUNT(*) >= ${minTrades}
    ) metrics ON w.address = metrics.wallet_address
    SET
      w.flag_bundler            = IF(metrics.same_block_pct > 0.05, 1, 0),
      w.bundler_same_block_pct  = ROUND(metrics.same_block_pct * 100, 2),
      w.bundler_same_block_count = metrics.same_block_trades
  `);

  const stats = await query<{ bundlers: number }>(
    `SELECT SUM(CASE WHEN flag_bundler = 1 THEN 1 ELSE 0 END) AS bundlers FROM wallets`
  );

  const result = { updated: 0, bundlers: Number(stats[0]?.bundlers) || 0 };
  logger.info('FLAG_BUNDLER updated', result);
  return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// FLAG_CREATOR_FUNDED
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Detecta wallets que receberam fundos do deployer do token antes da primeira compra.
 * Requer que a tabela tokens tenha deployer_address preenchido.
 */
export async function updateCreatorFundedFlags(): Promise<{ flagged: number }> {
  logger.info('Updating FLAG_CREATOR_FUNDED...');

  // Detectar wallets financiadas pelo deployer antes da primeira compra
  const funded = await query<{ wallet_address: string; token_address: string; funded_at: Date }>(
    `SELECT DISTINCT
       t.wallet_address,
       tk.address AS token_address,
       t.timestamp AS funded_at
     FROM transactions t
     JOIN tokens tk
       ON tk.deployer_address = t.from_address
     JOIN (
       SELECT wallet_address, token_out_address, MIN(timestamp) AS first_buy_at
       FROM swap_events
       WHERE is_long_trade IS NOT NULL
       GROUP BY wallet_address, token_out_address
     ) first_buy
       ON first_buy.wallet_address = t.wallet_address
       AND first_buy.token_out_address = tk.address
       AND t.timestamp < first_buy.first_buy_at
     WHERE t.tx_type IN ('transfer', 'receive')
       AND tk.deployer_address IS NOT NULL
     LIMIT 1000`
  );

  let flagged = 0;
  for (const row of funded) {
    await execute(
      `UPDATE wallets
       SET flag_creator_funded = 1,
           creator_funded_token = ?,
           creator_funded_at    = ?
       WHERE address = ? AND flag_creator_funded = 0`,
      [row.token_address, row.funded_at, row.wallet_address]
    );
    flagged++;
  }

  logger.info('FLAG_CREATOR_FUNDED updated', { flagged });
  return { flagged };
}

// ─────────────────────────────────────────────────────────────────────────────
// FLAG_SYBIL
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Detecta clusters de wallets coordenadas (Sybil).
 * Score >= 3 pontos em sinais de coordenação:
 *   - Funding comum:        2 pts
 *   - Timing sincronizado:  2 pts (mesmo token, diferença <= 30s, >= 3 ocorrências)
 *   - Tokens em comum:      1 pt  (Jaccard >= 0.5)
 */
export async function updateSybilFlags(): Promise<{ clusters: number; flagged: number }> {
  logger.info('Updating FLAG_SYBIL...');

  // Detectar pares com timing sincronizado (2 pts)
  const syncPairs = await query<{
    wallet_a: string;
    wallet_b: string;
    sync_trade_count: number;
  }>(
    `SELECT
       a.wallet_address AS wallet_a,
       b.wallet_address AS wallet_b,
       COUNT(*)          AS sync_trade_count
     FROM swap_events a
     JOIN swap_events b
       ON a.token_out_address = b.token_out_address
       AND a.wallet_address != b.wallet_address
       AND ABS(TIMESTAMPDIFF(SECOND, a.timestamp, b.timestamp)) <= 30
       AND a.wallet_address < b.wallet_address
       AND a.timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
     WHERE a.is_long_trade = 1 AND b.is_long_trade = 1
     GROUP BY a.wallet_address, b.wallet_address
     HAVING COUNT(*) >= 3
     ORDER BY COUNT(*) DESC
     LIMIT 500`
  );

  // Detectar pares com funding comum (2 pts)
  const fundingPairs = await query<{
    wallet_a: string;
    wallet_b: string;
    common_funder: string;
  }>(
    `SELECT
       t1.wallet_address AS wallet_a,
       t2.wallet_address AS wallet_b,
       t1.from_address   AS common_funder
     FROM transactions t1
     JOIN transactions t2
       ON t1.from_address = t2.from_address
       AND t1.wallet_address != t2.wallet_address
       AND t1.wallet_address < t2.wallet_address
       AND t1.tx_type = 'receive'
       AND t2.tx_type = 'receive'
     WHERE t1.timestamp >= DATE_SUB(NOW(), INTERVAL 90 DAY)
     GROUP BY t1.wallet_address, t2.wallet_address, t1.from_address
     HAVING COUNT(*) >= 1
     LIMIT 500`
  );

  // Construir mapa de pontuação por par
  const pairScores = new Map<string, { score: number; funder?: string; syncCount?: number }>();

  for (const pair of syncPairs) {
    const key = `${pair.wallet_a}:${pair.wallet_b}`;
    const existing = pairScores.get(key) || { score: 0 };
    pairScores.set(key, { ...existing, score: existing.score + 2, syncCount: pair.sync_trade_count });
  }

  for (const pair of fundingPairs) {
    const key = `${pair.wallet_a}:${pair.wallet_b}`;
    const existing = pairScores.get(key) || { score: 0 };
    pairScores.set(key, { ...existing, score: existing.score + 2, funder: pair.common_funder });
  }

  // Identificar pares com score >= 3 (Sybil confirmado)
  let clusters = 0;
  let flagged = 0;

  for (const [key, data] of pairScores.entries()) {
    if (data.score < 3) continue;

    const [walletA, walletB] = key.split(':');

    // Criar ou atualizar cluster
    const clusterResult = await execute(
      `INSERT INTO sybil_clusters (wallet_count, common_funder, sync_trade_count, jaccard_avg)
       VALUES (2, ?, ?, NULL)`,
      [data.funder || null, data.syncCount || null]
    );

    const clusterId = clusterResult.insertId;
    clusters++;

    // Adicionar membros ao cluster
    const signals = JSON.stringify({
      timing_sync: data.syncCount || 0,
      common_funding: data.funder ? 1 : 0,
      total_score: data.score,
    });

    for (const wallet of [walletA, walletB]) {
      try {
        await execute(
          `INSERT IGNORE INTO sybil_cluster_members (cluster_id, wallet_address, signals_matched)
           VALUES (?, ?, ?)`,
          [clusterId, wallet, signals]
        );

        await execute(
          `UPDATE wallets SET flag_sybil = 1, sybil_cluster_id = ? WHERE address = ?`,
          [clusterId, wallet]
        );
        flagged++;
      } catch {
        // Ignorar conflito de FK se wallet não existir
      }
    }
  }

  logger.info('FLAG_SYBIL updated', { clusters, flagged });
  return { clusters, flagged };
}

// ─────────────────────────────────────────────────────────────────────────────
// is_disqualified — Consolidação
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Recalcula is_disqualified para todas as wallets com base nas flags ativas.
 * is_disqualified = 1 se qualquer flag estiver ativa.
 */
export async function updateDisqualifiedStatus(): Promise<{ disqualified: number }> {
  await ddl(`
    UPDATE wallets
    SET is_disqualified = (
      flag_scalper = 1 OR
      flag_bundler = 1 OR
      flag_creator_funded = 1 OR
      flag_sybil = 1
    ),
    flags_updated_at = NOW()
  `);

  const stats = await query<{ disqualified: number }>(
    `SELECT SUM(CASE WHEN is_disqualified = 1 THEN 1 ELSE 0 END) AS disqualified FROM wallets`
  );

  const result = { disqualified: Number(stats[0]?.disqualified) || 0 };
  logger.info('is_disqualified recalculated', result);
  return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// Job Principal — Executar todas as flags
// ─────────────────────────────────────────────────────────────────────────────

export async function runDailyFlagsUpdate(): Promise<void> {
  logger.info('Starting daily flags update...');

  try {
    const scalperResult  = await updateScalperFlags();
    const bundlerResult  = await updateBundlerFlags();
    const creatorResult  = await updateCreatorFundedFlags();
    const sybilResult    = await updateSybilFlags();
    const disqResult     = await updateDisqualifiedStatus();

    logger.info('Daily flags update completed', {
      scalpers:     scalperResult.scalpers,
      bundlers:     bundlerResult.bundlers,
      creator_funded: creatorResult.flagged,
      sybil_clusters: sybilResult.clusters,
      sybil_wallets:  sybilResult.flagged,
      total_disqualified: disqResult.disqualified,
    });
  } catch (error) {
    logger.error('Daily flags update failed', { error: (error as Error).message });
    throw error;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Queries de Auditoria
// ─────────────────────────────────────────────────────────────────────────────

export async function getFlagsDistribution(): Promise<{
  scalpers: number;
  bundlers: number;
  creator_funded: number;
  sybil: number;
  total_disqualified: number;
  total_wallets: number;
}> {
  const result = await query<{
    scalpers: number;
    bundlers: number;
    creator_funded: number;
    sybil: number;
    total_disqualified: number;
    total_wallets: number;
  }>(
    `SELECT
       SUM(CASE WHEN flag_scalper = 1 THEN 1 ELSE 0 END)        AS scalpers,
       SUM(CASE WHEN flag_bundler = 1 THEN 1 ELSE 0 END)        AS bundlers,
       SUM(CASE WHEN flag_creator_funded = 1 THEN 1 ELSE 0 END) AS creator_funded,
       SUM(CASE WHEN flag_sybil = 1 THEN 1 ELSE 0 END)          AS sybil,
       SUM(CASE WHEN is_disqualified = 1 THEN 1 ELSE 0 END)     AS total_disqualified,
       COUNT(*)                                                   AS total_wallets
     FROM wallets`
  );

  return {
    scalpers:           Number(result[0]?.scalpers) || 0,
    bundlers:           Number(result[0]?.bundlers) || 0,
    creator_funded:     Number(result[0]?.creator_funded) || 0,
    sybil:              Number(result[0]?.sybil) || 0,
    total_disqualified: Number(result[0]?.total_disqualified) || 0,
    total_wallets:      Number(result[0]?.total_wallets) || 0,
  };
}

export async function getDisqualifiedWallets(
  limit = 100,
  offset = 0
): Promise<Array<{
  wallet_address: string;
  label: string | null;
  disqualification_reason: string;
  copiability_index: number | null;
  copiability_pct: number | null;
  details: Record<string, unknown>;
}>> {
  const rows = await query<{
    address: string;
    label: string | null;
    flag_scalper: number;
    flag_bundler: number;
    flag_creator_funded: number;
    flag_sybil: number;
    scalper_copiability_index: number | null;
    scalper_copiable_trades: number | null;
    scalper_total_trades: number | null;
    scalper_avg_holding_time: number | null;
    bundler_same_block_pct: number | null;
    bundler_same_block_count: number | null;
    creator_funded_token: string | null;
    creator_funded_at: Date | null;
    sybil_cluster_id: number | null;
  }>(
    `SELECT address, label,
            flag_scalper, flag_bundler, flag_creator_funded, flag_sybil,
            scalper_copiability_index, scalper_copiable_trades, scalper_total_trades, scalper_avg_holding_time,
            bundler_same_block_pct, bundler_same_block_count,
            creator_funded_token, creator_funded_at,
            sybil_cluster_id
     FROM wallets
     WHERE is_disqualified = 1
     ORDER BY flags_updated_at DESC
     LIMIT ${limit} OFFSET ${offset}`
  );

  return rows.map((row) => {
    const reason =
      row.flag_scalper       ? 'SCALPER' :
      row.flag_bundler       ? 'BUNDLER' :
      row.flag_creator_funded ? 'CREATOR_FUNDED' :
      row.flag_sybil         ? 'SYBIL' : 'UNKNOWN';

    const details: Record<string, unknown> = {};
    if (row.flag_scalper) {
      details.copiable_trades   = row.scalper_copiable_trades;
      details.total_trades      = row.scalper_total_trades;
      details.avg_holding_time_s = row.scalper_avg_holding_time;
    }
    if (row.flag_bundler) {
      details.same_block_pct   = row.bundler_same_block_pct;
      details.same_block_count = row.bundler_same_block_count;
    }
    if (row.flag_creator_funded) {
      details.funded_by_token = row.creator_funded_token;
      details.funded_at       = row.creator_funded_at;
    }
    if (row.flag_sybil) {
      details.cluster_id = row.sybil_cluster_id;
    }

    return {
      wallet_address: row.address,
      label: row.label,
      disqualification_reason: reason,
      copiability_index: row.scalper_copiability_index,
      copiability_pct: row.scalper_copiability_index !== null
        ? Math.round(Number(row.scalper_copiability_index) * 10000) / 100
        : null,
      details,
    };
  });
}
