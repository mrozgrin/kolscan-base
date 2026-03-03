import { query, execute } from '../database/connection';
import { logger } from '../utils/logger';
import { getPeriodStart, calculateWinRate } from '../utils/helpers';
import { KolMetrics, LeaderboardEntry } from '../types';
import { config } from '../config';

// ─────────────────────────────────────────────────────────────────────────────
// Follow Score — Fórmula
// ─────────────────────────────────────────────────────────────────────────────
//
//   follow_score = (win_rate_score * winRateWeight)
//                + (holding_score  * holdingWeight)
//
//   win_rate_score  = win_rate (0-100)
//   holding_score:
//     Se holding_avg >= scalpingThreshold → min(100, holding_avg / maxHolding * 100)
//     Se holding_avg <  scalpingThreshold → penalidade proporcional (máx 50 pts)
//
// Pesos padrão: 50% win rate + 50% holding time
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Calcula o follow score em TypeScript (usado ao salvar kol_metrics)
 */
function calculateFollowScore(
  winRate: number,
  holdingTimeAvgS: number | null
): number {
  const {
    followScoreWinRateWeight: winWeight,
    followScoreHoldingWeight: holdWeight,
    followScoreMaxHoldingSeconds: maxHolding,
    scalpingThresholdSeconds: scalpingThreshold,
  } = config.indexer;

  const winRateScore = Math.min(100, Math.max(0, winRate));

  let holdingScore = 0;
  if (holdingTimeAvgS !== null && holdingTimeAvgS > 0) {
    if (holdingTimeAvgS < scalpingThreshold) {
      holdingScore = (holdingTimeAvgS / scalpingThreshold) * 50;
    } else {
      holdingScore = Math.min(100, (holdingTimeAvgS / maxHolding) * 100);
    }
  }

  const score = winRateScore * winWeight + holdingScore * holdWeight;
  return Math.round(Math.min(100, Math.max(0, score)) * 100) / 100;
}

/**
 * Gera a expressão SQL do follow score para ser usada diretamente nas queries.
 * Evita dependência do JOIN com kol_metrics (que pode falhar por diferença de period_start).
 *
 * Parâmetros injetados diretamente (são constantes numéricas seguras, sem input do usuário):
 *   winWeight, holdWeight, maxHolding, scalpingThreshold
 */
function followScoreSql(holdingAvgExpr: string, winRateExpr: string): string {
  const { followScoreWinRateWeight: ww, followScoreHoldingWeight: hw,
          followScoreMaxHoldingSeconds: maxH, scalpingThresholdSeconds: thr } = config.indexer;

  // holding_score SQL:
  //   Se avg_holding >= threshold → LEAST(100, avg_holding / maxH * 100)
  //   Se avg_holding <  threshold → (avg_holding / threshold) * 50
  //   Se NULL → 0
  const holdingScoreSql = `
    CASE
      WHEN ${holdingAvgExpr} IS NULL OR ${holdingAvgExpr} <= 0 THEN 0
      WHEN ${holdingAvgExpr} < ${thr}
        THEN (${holdingAvgExpr} / ${thr}) * 50
      ELSE LEAST(100, ${holdingAvgExpr} / ${maxH} * 100)
    END`;

  return `LEAST(100, GREATEST(0,
    (${winRateExpr}) * ${ww}
    + (${holdingScoreSql}) * ${hw}
  ))`;
}

/**
 * Formata segundos para exibição legível (ex: "2m 30s", "1h 15m", "3d 2h")
 */
export function formatHoldingTime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) {
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    return s > 0 ? `${m}m ${s}s` : `${m}m`;
  }
  if (seconds < 86400) {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    return m > 0 ? `${h}h ${m}m` : `${h}h`;
  }
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  return h > 0 ? `${d}d ${h}h` : `${d}d`;
}

/**
 * Calcula e persiste as métricas de um KOL para um período específico
 */
export async function calculateKolMetrics(
  walletAddress: string,
  period: 'daily' | 'weekly' | 'monthly' | 'all_time'
): Promise<KolMetrics | null> {
  const periodStart = getPeriodStart(period);
  const periodEnd = new Date();
  const scalpingThreshold = config.indexer.scalpingThresholdSeconds;

  try {
    const stats = await query<{
      wins: number;
      losses: number;
      total_trades: number;
      profit_usd: number;
      avg_trade_size: number;
      best_trade: number;
      worst_trade: number;
      unique_tokens: number;
      holding_time_avg: number | null;
      scalping_trades: number;
      total_invested_usd: number;
    }>(
      `SELECT
         COUNT(CASE WHEN is_win = 1 THEN 1 END)                          AS wins,
         COUNT(CASE WHEN is_win = 0 THEN 1 END)                          AS losses,
         COUNT(*)                                                          AS total_trades,
         COALESCE(SUM(pnl), 0)                                            AS profit_usd,
         COALESCE(AVG(ABS(value_usd)), 0)                                 AS avg_trade_size,
         COALESCE(MAX(pnl), 0)                                            AS best_trade,
         COALESCE(MIN(pnl), 0)                                            AS worst_trade,
         COUNT(DISTINCT token_out_address)                                AS unique_tokens,
         AVG(holding_time_s)                                              AS holding_time_avg,
         COUNT(CASE WHEN holding_time_s IS NOT NULL
                     AND holding_time_s < ? THEN 1 END)                  AS scalping_trades,
         COALESCE(SUM(CASE WHEN value_usd > 0 THEN value_usd ELSE 0 END), 0) AS total_invested_usd
       FROM swap_events
       WHERE wallet_address = ?
         AND timestamp >= ?
         AND timestamp <= ?
         AND pnl IS NOT NULL`,
      [scalpingThreshold, walletAddress.toLowerCase(), periodStart, periodEnd]
    );

    if (!stats.length) return null;

    const s = stats[0];
    const wins = Number(s.wins) || 0;
    const losses = Number(s.losses) || 0;
    const totalTrades = Number(s.total_trades) || 0;
    const profitUsd = Number(s.profit_usd) || 0;
    const totalInvested = Number(s.total_invested_usd) || 0;
    const holdingTimeAvg = s.holding_time_avg !== null ? Math.round(Number(s.holding_time_avg)) : null;
    const scalpingTrades = Number(s.scalping_trades) || 0;

    if (totalTrades < config.indexer.minTradesForKol) return null;

    const winRate = calculateWinRate(wins, totalTrades);
    const scalpingRate = totalTrades > 0
      ? Math.round((scalpingTrades / totalTrades) * 10000) / 100
      : 0;
    const followScore = calculateFollowScore(winRate, holdingTimeAvg);
    // profit_pct: lucro relativo ao capital investido no período
    const profitPct = totalInvested > 0
      ? Math.round((profitUsd / totalInvested) * 10000) / 100
      : 0;

    const metrics: KolMetrics = {
      wallet_address: walletAddress,
      period,
      wins,
      losses,
      total_trades: totalTrades,
      profit_eth: 0,
      profit_usd: profitUsd,
      win_rate: winRate,
      avg_trade_size_eth: 0,
      best_trade_pnl: Number(s.best_trade) || undefined,
      worst_trade_pnl: Number(s.worst_trade) || undefined,
      unique_tokens_traded: Number(s.unique_tokens) || 0,
      last_updated: new Date(),
    };

    await execute(
      `INSERT INTO kol_metrics
         (wallet_address, period, period_start, period_end,
          wins, losses, total_trades, profit_eth, profit_usd, win_rate,
          avg_trade_size_eth, best_trade_pnl, worst_trade_pnl,
          unique_tokens_traded, last_updated,
          holding_time_avg_s, holding_time_median_s,
          scalping_trades, scalping_rate, follow_score)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         wins                  = VALUES(wins),
         losses                = VALUES(losses),
         total_trades          = VALUES(total_trades),
         profit_eth            = VALUES(profit_eth),
         profit_usd            = VALUES(profit_usd),
         win_rate              = VALUES(win_rate),
         avg_trade_size_eth    = VALUES(avg_trade_size_eth),
         best_trade_pnl        = VALUES(best_trade_pnl),
         worst_trade_pnl       = VALUES(worst_trade_pnl),
         unique_tokens_traded  = VALUES(unique_tokens_traded),
         last_updated          = NOW(),
         holding_time_avg_s    = VALUES(holding_time_avg_s),
         holding_time_median_s = VALUES(holding_time_median_s),
         scalping_trades       = VALUES(scalping_trades),
         scalping_rate         = VALUES(scalping_rate),
         follow_score          = VALUES(follow_score)`,
      [
        walletAddress,
        period,
        periodStart,
        periodEnd,
        metrics.wins,
        metrics.losses,
        metrics.total_trades,
        metrics.profit_eth,
        metrics.profit_usd,
        metrics.win_rate,
        metrics.avg_trade_size_eth,
        metrics.best_trade_pnl ?? null,
        metrics.worst_trade_pnl ?? null,
        metrics.unique_tokens_traded,
        holdingTimeAvg,
        holdingTimeAvg, // median ≈ avg por ora
        scalpingTrades,
        scalpingRate,
        followScore,
      ]
    );

    return metrics;
  } catch (error) {
    logger.error('Error calculating KOL metrics', {
      wallet: walletAddress,
      period,
      error: (error as Error).message,
    });
    return null;
  }
}

/**
 * Atualiza as métricas de todos os KOLs ativos
 */
export async function updateAllKolMetrics(): Promise<void> {
  logger.info('Updating KOL metrics for all active wallets...');

  try {
    const activeWallets = await query<{ address: string }>(
      `SELECT wallet_address AS address
       FROM swap_events
       WHERE timestamp >= NOW() - INTERVAL 30 DAY
       GROUP BY wallet_address
       HAVING COUNT(*) >= ?`,
      [config.indexer.minTradesForKol]
    );

    logger.info(`Found ${activeWallets.length} active wallets to update`);

    const periods: Array<'daily' | 'weekly' | 'monthly' | 'all_time'> = [
      'daily', 'weekly', 'monthly', 'all_time',
    ];

    for (const wallet of activeWallets) {
      for (const period of periods) {
        await calculateKolMetrics(wallet.address, period);
      }
    }

    logger.info('KOL metrics update completed');
  } catch (error) {
    logger.error('Error updating KOL metrics', { error: (error as Error).message });
  }
}

/**
 * Obtém o leaderboard para um período específico.
 *
 * O follow_score é calculado INLINE na query SQL para garantir que sempre
 * reflita os dados reais de swap_events, sem depender de um JOIN com
 * kol_metrics que pode falhar por diferença de period_start.
 */
export async function getLeaderboard(
  period: 'daily' | 'weekly' | 'monthly' | 'all_time',
  limit: number = 100,
  offset: number = 0
): Promise<LeaderboardEntry[]> {
  const periodStart = getPeriodStart(period);
  const thr = config.indexer.scalpingThresholdSeconds;

  // Expressões reutilizadas na query
  const winRateExpr = `COUNT(CASE WHEN se.is_win = 1 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)`;
  const holdingAvgExpr = `AVG(se.holding_time_s)`;
  const followScoreExpr = followScoreSql(holdingAvgExpr, winRateExpr);

  try {
    const results = await query<{
      wallet_address: string;
      label: string | null;
      wins: number;
      losses: number;
      total_trades: number;
      profit_usd: number;
      total_invested_usd: number;
      win_rate: number;
      holding_time_avg_s: number | null;
      scalping_rate: number;
      follow_score: number;
    }>(
      `SELECT
         se.wallet_address,
         w.label,
         COUNT(CASE WHEN se.is_win = 1 THEN 1 END)                                  AS wins,
         COUNT(CASE WHEN se.is_win = 0 THEN 1 END)                                  AS losses,
         COUNT(*)                                                                     AS total_trades,
         COALESCE(SUM(se.pnl), 0)                                                    AS profit_usd,
         COALESCE(SUM(CASE WHEN se.value_usd > 0 THEN se.value_usd ELSE 0 END), 0)  AS total_invested_usd,
         COALESCE(${winRateExpr}, 0)                                                 AS win_rate,
         ${holdingAvgExpr}                                                            AS holding_time_avg_s,
         COALESCE(
           COUNT(CASE WHEN se.holding_time_s IS NOT NULL
                       AND se.holding_time_s < ${thr} THEN 1 END) * 100.0
           / NULLIF(COUNT(*), 0), 0)                                                 AS scalping_rate,
         COALESCE(${followScoreExpr}, 0)                                             AS follow_score
       FROM swap_events se
       LEFT JOIN wallets w ON w.address = se.wallet_address
       WHERE se.timestamp >= ?
         AND se.pnl IS NOT NULL
       GROUP BY se.wallet_address, w.label
       HAVING COUNT(*) >= ?
       ORDER BY SUM(se.pnl) DESC
       LIMIT ${limit} OFFSET ${offset}`,
      [periodStart, config.indexer.minTradesForKol]
    );

    return results.map((row, index) => {
      const holdingAvg = row.holding_time_avg_s !== null
        ? Math.round(Number(row.holding_time_avg_s))
        : undefined;

      const profitUsd = Number(row.profit_usd) || 0;
      const totalInvested = Number(row.total_invested_usd) || 0;
      const profitPct = totalInvested > 0
        ? Math.round((profitUsd / totalInvested) * 10000) / 100
        : 0;

      return {
        rank: offset + index + 1,
        wallet_address: row.wallet_address,
        label: row.label || undefined,
        wins: Number(row.wins) || 0,
        losses: Number(row.losses) || 0,
        total_trades: Number(row.total_trades) || 0,
        profit_eth: 0,
        profit_usd: profitUsd,
        profit_pct: profitPct,
        win_rate: Math.round(Number(row.win_rate) * 100) / 100,
        period,
        holding_time_avg_s: holdingAvg,
        holding_time_formatted: holdingAvg ? formatHoldingTime(holdingAvg) : null,
        scalping_rate: Math.round(Number(row.scalping_rate) * 100) / 100,
        follow_score: Math.round(Number(row.follow_score) * 100) / 100,
      } as LeaderboardEntry & {
        profit_pct: number;
        holding_time_formatted: string | null;
      };
    });
  } catch (error) {
    logger.error('Error getting leaderboard', { period, error: (error as Error).message });
    return [];
  }
}

/**
 * Obtém os detalhes completos de um KOL
 */
export async function getKolDetails(walletAddress: string): Promise<{
  wallet: {
    address: string;
    label?: string;
    first_seen: Date;
    last_seen: Date;
    total_transactions: number;
  };
  metrics: {
    daily: KolMetrics | null;
    weekly: KolMetrics | null;
    monthly: KolMetrics | null;
    all_time: KolMetrics | null;
  };
  holding_analysis: {
    scalping_threshold_s: number;
    scalping_threshold_formatted: string;
    avg_holding_s: number | null;
    avg_holding_formatted: string | null;
    follow_score: number;
    follow_score_label: string;
    profit_pct_all_time: number;
  };
  recent_swaps: Array<{
    tx_hash: string;
    timestamp: Date;
    dex_name: string;
    token_in_symbol: string;
    token_out_symbol: string;
    value_usd: number | null;
    pnl: number | null;
    is_win: boolean | null;
    holding_time_s: number | null;
    holding_time_formatted: string | null;
  }>;
} | null> {
  const normalizedAddress = walletAddress.toLowerCase();

  const walletData = await query<{
    address: string;
    label: string | null;
    first_seen: Date;
    last_seen: Date;
    total_transactions: number;
  }>(
    'SELECT address, label, first_seen, last_seen, total_transactions FROM wallets WHERE address = ?',
    [normalizedAddress]
  );

  if (!walletData.length) return null;

  const [daily, weekly, monthly, all_time] = await Promise.all([
    calculateKolMetrics(normalizedAddress, 'daily'),
    calculateKolMetrics(normalizedAddress, 'weekly'),
    calculateKolMetrics(normalizedAddress, 'monthly'),
    calculateKolMetrics(normalizedAddress, 'all_time'),
  ]);

  // Calcular follow_score e profit_pct inline (sem depender de kol_metrics)
  const thr = config.indexer.scalpingThresholdSeconds;
  const winRateExpr = `COUNT(CASE WHEN is_win = 1 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)`;
  const holdingAvgExpr = `AVG(holding_time_s)`;
  const followScoreExpr = followScoreSql(holdingAvgExpr, winRateExpr);

  const liveStats = await query<{
    holding_time_avg_s: number | null;
    follow_score: number;
    total_invested_usd: number;
    profit_usd: number;
    scalping_rate: number;
  }>(
    `SELECT
       ${holdingAvgExpr}                                                              AS holding_time_avg_s,
       COALESCE(${followScoreExpr}, 0)                                               AS follow_score,
       COALESCE(SUM(CASE WHEN value_usd > 0 THEN value_usd ELSE 0 END), 0)           AS total_invested_usd,
       COALESCE(SUM(pnl), 0)                                                          AS profit_usd,
       COALESCE(
         COUNT(CASE WHEN holding_time_s IS NOT NULL
                     AND holding_time_s < ${thr} THEN 1 END) * 100.0
         / NULLIF(COUNT(*), 0), 0)                                                    AS scalping_rate
     FROM swap_events
     WHERE wallet_address = ?
       AND pnl IS NOT NULL`,
    [normalizedAddress]
  );

  const avgHolding = liveStats[0]?.holding_time_avg_s
    ? Math.round(Number(liveStats[0].holding_time_avg_s))
    : null;
  const followScore = liveStats[0]?.follow_score
    ? Math.round(Number(liveStats[0].follow_score) * 100) / 100
    : 0;
  const totalInvested = Number(liveStats[0]?.total_invested_usd) || 0;
  const profitUsd = Number(liveStats[0]?.profit_usd) || 0;
  const profitPct = totalInvested > 0
    ? Math.round((profitUsd / totalInvested) * 10000) / 100
    : 0;

  const followScoreLabel =
    followScore >= 80 ? 'Excelente — altamente recomendado seguir' :
    followScore >= 60 ? 'Bom — vale a pena acompanhar' :
    followScore >= 40 ? 'Moderado — siga com cautela' :
    followScore >= 20 ? 'Fraco — scalper ou baixo win rate' :
                        'Não recomendado — alto risco';

  const recentSwaps = await query<{
    tx_hash: string;
    timestamp: Date;
    dex_name: string;
    token_in_symbol: string;
    token_out_symbol: string;
    value_usd: number | null;
    pnl: number | null;
    is_win: boolean | null;
    holding_time_s: number | null;
  }>(
    `SELECT tx_hash, timestamp, dex_name, token_in_symbol, token_out_symbol,
            value_usd, pnl, is_win, holding_time_s
     FROM swap_events
     WHERE wallet_address = ?
     ORDER BY timestamp DESC
     LIMIT 50`,
    [normalizedAddress]
  );

  const wallet = walletData[0];
  const scalpingThreshold = config.indexer.scalpingThresholdSeconds;

  return {
    wallet: {
      address: wallet.address,
      label: wallet.label || undefined,
      first_seen: wallet.first_seen,
      last_seen: wallet.last_seen,
      total_transactions: wallet.total_transactions,
    },
    metrics: { daily, weekly, monthly, all_time },
    holding_analysis: {
      scalping_threshold_s: scalpingThreshold,
      scalping_threshold_formatted: formatHoldingTime(scalpingThreshold),
      avg_holding_s: avgHolding,
      avg_holding_formatted: avgHolding ? formatHoldingTime(avgHolding) : null,
      follow_score: followScore,
      follow_score_label: followScoreLabel,
      profit_pct_all_time: profitPct,
    },
    recent_swaps: recentSwaps.map((s) => ({
      ...s,
      holding_time_formatted: s.holding_time_s ? formatHoldingTime(s.holding_time_s) : null,
    })),
  };
}

/**
 * Obtém estatísticas gerais da plataforma
 */
export async function getPlatformStats(): Promise<{
  total_wallets: number;
  total_swaps: number;
  total_volume_usd: number;
  active_wallets_24h: number;
  avg_follow_score: number;
  top_dexes: Array<{ dex_name: string; swap_count: number }>;
}> {
  const [generalStats, topDexes] = await Promise.all([
    query<{
      total_wallets: number;
      total_swaps: number;
      total_volume_usd: number;
      active_wallets_24h: number;
      avg_follow_score: number;
    }>(
      `SELECT
         (SELECT COUNT(*) FROM wallets)                                                         AS total_wallets,
         (SELECT COUNT(*) FROM swap_events)                                                     AS total_swaps,
         (SELECT COALESCE(SUM(ABS(value_usd)), 0) FROM swap_events WHERE value_usd IS NOT NULL) AS total_volume_usd,
         (SELECT COUNT(DISTINCT wallet_address) FROM swap_events
          WHERE timestamp >= NOW() - INTERVAL 24 HOUR)                                         AS active_wallets_24h,
         (SELECT COALESCE(AVG(follow_score), 0) FROM kol_metrics
          WHERE period = 'all_time')                                                            AS avg_follow_score`
    ),
    query<{ dex_name: string; swap_count: number }>(
      `SELECT dex_name, COUNT(*) AS swap_count
       FROM swap_events
       WHERE timestamp >= NOW() - INTERVAL 7 DAY
       GROUP BY dex_name
       ORDER BY COUNT(*) DESC
       LIMIT 10`
    ),
  ]);

  const stats = generalStats[0] || {
    total_wallets: 0,
    total_swaps: 0,
    total_volume_usd: 0,
    active_wallets_24h: 0,
    avg_follow_score: 0,
  };

  return {
    total_wallets: Number(stats.total_wallets) || 0,
    total_swaps: Number(stats.total_swaps) || 0,
    total_volume_usd: Number(stats.total_volume_usd) || 0,
    active_wallets_24h: Number(stats.active_wallets_24h) || 0,
    avg_follow_score: Math.round(Number(stats.avg_follow_score) * 100) / 100,
    top_dexes: topDexes.map((d) => ({
      dex_name: d.dex_name,
      swap_count: Number(d.swap_count) || 0,
    })),
  };
}
