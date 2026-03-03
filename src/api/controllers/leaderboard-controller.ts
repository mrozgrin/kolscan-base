import { Request, Response, NextFunction } from 'express';
import { getLeaderboard } from '../../services/metrics-service';
import { ApiResponse, LeaderboardEntry } from '../../types';
import { logger } from '../../utils/logger';
import { config } from '../../config';

// Cache simples em memória para o leaderboard
const leaderboardCache = new Map<
  string,
  { data: LeaderboardEntry[]; timestamp: number }
>();

/**
 * GET /api/leaderboard
 * Retorna o leaderboard dos KOLs para um período específico.
 *
 * Query params:
 *   period   — daily | weekly | monthly | all_time (default: daily)
 *   sort_by  — profit_usd | follow_score | win_rate (default: profit_usd)
 *   limit    — 1-200 (default: 50)
 *   page     — página (default: 1)
 */
export async function getLeaderboardHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const period = (req.query.period as 'daily' | 'weekly' | 'monthly' | 'all_time') || 'daily';
    const sortBy = (req.query.sort_by as string) || 'profit_usd';
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const page = Math.max(parseInt(req.query.page as string) || 1, 1);
    const offset = (page - 1) * limit;

    const cacheKey = `${period}_${sortBy}_${limit}_${offset}`;
    const cacheTtlMs = config.cache.leaderboardTtl * 1000;

    // Verificar cache
    const cached = leaderboardCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < cacheTtlMs) {
      res.json({
        success: true,
        data: cached.data,
        meta: buildMeta(period, sortBy),
        pagination: { page, limit, total: cached.data.length, total_pages: Math.ceil(cached.data.length / limit) },
      } as ApiResponse<LeaderboardEntry[]>);
      return;
    }

    let leaderboard = await getLeaderboard(period, limit, offset);

    // Reordenar client-side se necessário (a query já ordena por profit_usd)
    if (sortBy === 'follow_score') {
      leaderboard = leaderboard
        .sort((a, b) => b.follow_score - a.follow_score)
        .map((entry, i) => ({ ...entry, rank: offset + i + 1 }));
    } else if (sortBy === 'win_rate') {
      leaderboard = leaderboard
        .sort((a, b) => b.win_rate - a.win_rate)
        .map((entry, i) => ({ ...entry, rank: offset + i + 1 }));
    }

    leaderboardCache.set(cacheKey, { data: leaderboard, timestamp: Date.now() });

    res.json({
      success: true,
      data: leaderboard,
      meta: buildMeta(period, sortBy),
      pagination: { page, limit, total: leaderboard.length, total_pages: Math.ceil(leaderboard.length / limit) },
    } as ApiResponse<LeaderboardEntry[]>);
  } catch (error) {
    logger.error('Error in getLeaderboard', { error: (error as Error).message });
    next(error);
  }
}

/**
 * Constrói o objeto meta com informações sobre os parâmetros de configuração
 */
function buildMeta(period: string, sortBy: string) {
  return {
    period,
    sort_by: sortBy,
    scalping_threshold_s: config.indexer.scalpingThresholdSeconds,
    follow_score_info: {
      description: 'Nota 0-100 que indica o quanto vale a pena seguir o trader',
      win_rate_weight: config.indexer.followScoreWinRateWeight,
      holding_weight: config.indexer.followScoreHoldingWeight,
      max_holding_seconds: config.indexer.followScoreMaxHoldingSeconds,
      formula: 'follow_score = (win_rate * win_rate_weight) + (holding_score * holding_weight)',
      holding_score_formula: 'Se holding >= scalping_threshold: min(100, holding_avg / max_holding * 100). Se holding < scalping_threshold: penalidade proporcional (máx 50pts)',
    },
  };
}
