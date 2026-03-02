import { query } from '../database/connection';
import { logger } from '../utils/logger';
import { getPeriodStart, calculateWinRate } from '../utils/helpers';
import { KolMetrics, LeaderboardEntry } from '../types';
import { config } from '../config';

/**
 * Calcula e atualiza as métricas de um KOL para um período específico
 */
export async function calculateKolMetrics(
  walletAddress: string,
  period: 'daily' | 'weekly' | 'monthly' | 'all_time'
): Promise<KolMetrics | null> {
  const periodStart = getPeriodStart(period);
  const periodEnd = new Date();

  try {
    // Buscar estatísticas de swaps do período
    const stats = await query<{
      wins: string;
      losses: string;
      total_trades: string;
      profit_usd: string;
      avg_trade_size: string;
      best_trade: string;
      worst_trade: string;
      unique_tokens: string;
    }>(
      `SELECT
         COUNT(CASE WHEN is_win = true THEN 1 END)::TEXT as wins,
         COUNT(CASE WHEN is_win = false THEN 1 END)::TEXT as losses,
         COUNT(*)::TEXT as total_trades,
         COALESCE(SUM(pnl), 0)::TEXT as profit_usd,
         COALESCE(AVG(ABS(value_usd)), 0)::TEXT as avg_trade_size,
         COALESCE(MAX(pnl), 0)::TEXT as best_trade,
         COALESCE(MIN(pnl), 0)::TEXT as worst_trade,
         COUNT(DISTINCT token_out_address)::TEXT as unique_tokens
       FROM swap_events
       WHERE wallet_address = $1
         AND timestamp >= $2
         AND timestamp <= $3
         AND pnl IS NOT NULL`,
      [walletAddress.toLowerCase(), periodStart, periodEnd]
    );

    if (!stats.length) return null;

    const s = stats[0];
    const wins = parseInt(s.wins) || 0;
    const losses = parseInt(s.losses) || 0;
    const totalTrades = parseInt(s.total_trades) || 0;
    const profitUsd = parseFloat(s.profit_usd) || 0;

    if (totalTrades < config.indexer.minTradesForKol) return null;

    const metrics: KolMetrics = {
      wallet_address: walletAddress,
      period,
      wins,
      losses,
      total_trades: totalTrades,
      profit_eth: 0, // TODO: calcular em ETH
      profit_usd: profitUsd,
      win_rate: calculateWinRate(wins, totalTrades),
      avg_trade_size_eth: 0,
      best_trade_pnl: parseFloat(s.best_trade) || undefined,
      worst_trade_pnl: parseFloat(s.worst_trade) || undefined,
      unique_tokens_traded: parseInt(s.unique_tokens) || 0,
      last_updated: new Date(),
    };

    // Salvar métricas no banco de dados
    await query(
      `INSERT INTO kol_metrics (
         wallet_address, period, period_start, period_end,
         wins, losses, total_trades, profit_eth, profit_usd, win_rate,
         avg_trade_size_eth, best_trade_pnl, worst_trade_pnl,
         unique_tokens_traded, last_updated
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
       ON CONFLICT (wallet_address, period, period_start)
       DO UPDATE SET
         wins = $5, losses = $6, total_trades = $7,
         profit_eth = $8, profit_usd = $9, win_rate = $10,
         avg_trade_size_eth = $11, best_trade_pnl = $12, worst_trade_pnl = $13,
         unique_tokens_traded = $14, last_updated = NOW()`,
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
        metrics.best_trade_pnl || null,
        metrics.worst_trade_pnl || null,
        metrics.unique_tokens_traded,
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
    // Buscar carteiras com atividade recente
    const activeWallets = await query<{ address: string }>(
      `SELECT DISTINCT wallet_address as address
       FROM swap_events
       WHERE timestamp >= NOW() - INTERVAL '30 days'
       GROUP BY wallet_address
       HAVING COUNT(*) >= $1`,
      [config.indexer.minTradesForKol]
    );

    logger.info(`Found ${activeWallets.length} active wallets to update`);

    const periods: Array<'daily' | 'weekly' | 'monthly' | 'all_time'> = [
      'daily',
      'weekly',
      'monthly',
      'all_time',
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
 * Obtém o leaderboard para um período específico
 */
export async function getLeaderboard(
  period: 'daily' | 'weekly' | 'monthly' | 'all_time',
  limit: number = 100,
  offset: number = 0
): Promise<LeaderboardEntry[]> {
  const periodStart = getPeriodStart(period);

  try {
    const results = await query<{
      wallet_address: string;
      label: string | null;
      wins: string;
      losses: string;
      total_trades: string;
      profit_eth: string;
      profit_usd: string;
      win_rate: string;
    }>(
      `SELECT
         se.wallet_address,
         w.label,
         COUNT(CASE WHEN se.is_win = true THEN 1 END)::TEXT as wins,
         COUNT(CASE WHEN se.is_win = false THEN 1 END)::TEXT as losses,
         COUNT(*)::TEXT as total_trades,
         COALESCE(SUM(se.pnl), 0)::TEXT as profit_usd,
         0::TEXT as profit_eth,
         CASE 
           WHEN COUNT(*) > 0 
           THEN (COUNT(CASE WHEN se.is_win = true THEN 1 END) * 100.0 / COUNT(*))
           ELSE 0
         END::TEXT as win_rate
       FROM swap_events se
       LEFT JOIN wallets w ON w.address = se.wallet_address
       WHERE se.timestamp >= $1
         AND se.pnl IS NOT NULL
       GROUP BY se.wallet_address, w.label
       HAVING COUNT(*) >= $2
       ORDER BY SUM(se.pnl) DESC NULLS LAST
       LIMIT $3 OFFSET $4`,
      [periodStart, config.indexer.minTradesForKol, limit, offset]
    );

    return results.map((row, index) => ({
      rank: offset + index + 1,
      wallet_address: row.wallet_address,
      label: row.label || undefined,
      wins: parseInt(row.wins) || 0,
      losses: parseInt(row.losses) || 0,
      total_trades: parseInt(row.total_trades) || 0,
      profit_eth: parseFloat(row.profit_eth) || 0,
      profit_usd: parseFloat(row.profit_usd) || 0,
      win_rate: parseFloat(row.win_rate) || 0,
      period,
    }));
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
  recent_swaps: Array<{
    tx_hash: string;
    timestamp: Date;
    dex_name: string;
    token_in_symbol: string;
    token_out_symbol: string;
    value_usd: number | null;
    pnl: number | null;
    is_win: boolean | null;
  }>;
} | null> {
  const normalizedAddress = walletAddress.toLowerCase();

  // Buscar dados da carteira
  const walletData = await query<{
    address: string;
    label: string | null;
    first_seen: Date;
    last_seen: Date;
    total_transactions: number;
  }>(
    'SELECT address, label, first_seen, last_seen, total_transactions FROM wallets WHERE address = $1',
    [normalizedAddress]
  );

  if (!walletData.length) return null;

  // Calcular métricas para todos os períodos
  const [daily, weekly, monthly, all_time] = await Promise.all([
    calculateKolMetrics(normalizedAddress, 'daily'),
    calculateKolMetrics(normalizedAddress, 'weekly'),
    calculateKolMetrics(normalizedAddress, 'monthly'),
    calculateKolMetrics(normalizedAddress, 'all_time'),
  ]);

  // Buscar swaps recentes
  const recentSwaps = await query<{
    tx_hash: string;
    timestamp: Date;
    dex_name: string;
    token_in_symbol: string;
    token_out_symbol: string;
    value_usd: number | null;
    pnl: number | null;
    is_win: boolean | null;
  }>(
    `SELECT tx_hash, timestamp, dex_name, token_in_symbol, token_out_symbol, value_usd, pnl, is_win
     FROM swap_events
     WHERE wallet_address = $1
     ORDER BY timestamp DESC
     LIMIT 50`,
    [normalizedAddress]
  );

  const wallet = walletData[0];

  return {
    wallet: {
      address: wallet.address,
      label: wallet.label || undefined,
      first_seen: wallet.first_seen,
      last_seen: wallet.last_seen,
      total_transactions: wallet.total_transactions,
    },
    metrics: { daily, weekly, monthly, all_time },
    recent_swaps: recentSwaps,
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
  top_dexes: Array<{ dex_name: string; swap_count: number }>;
}> {
  const [generalStats, topDexes] = await Promise.all([
    query<{
      total_wallets: string;
      total_swaps: string;
      total_volume_usd: string;
      active_wallets_24h: string;
    }>(
      `SELECT
         (SELECT COUNT(*) FROM wallets)::TEXT as total_wallets,
         (SELECT COUNT(*) FROM swap_events)::TEXT as total_swaps,
         (SELECT COALESCE(SUM(ABS(value_usd)), 0) FROM swap_events WHERE value_usd IS NOT NULL)::TEXT as total_volume_usd,
         (SELECT COUNT(DISTINCT wallet_address) FROM swap_events WHERE timestamp >= NOW() - INTERVAL '24 hours')::TEXT as active_wallets_24h`
    ),
    query<{ dex_name: string; swap_count: string }>(
      `SELECT dex_name, COUNT(*)::TEXT as swap_count
       FROM swap_events
       WHERE timestamp >= NOW() - INTERVAL '7 days'
       GROUP BY dex_name
       ORDER BY COUNT(*) DESC
       LIMIT 10`
    ),
  ]);

  const stats = generalStats[0] || {
    total_wallets: '0',
    total_swaps: '0',
    total_volume_usd: '0',
    active_wallets_24h: '0',
  };

  return {
    total_wallets: parseInt(stats.total_wallets) || 0,
    total_swaps: parseInt(stats.total_swaps) || 0,
    total_volume_usd: parseFloat(stats.total_volume_usd) || 0,
    active_wallets_24h: parseInt(stats.active_wallets_24h) || 0,
    top_dexes: topDexes.map((d) => ({
      dex_name: d.dex_name,
      swap_count: parseInt(d.swap_count) || 0,
    })),
  };
}
