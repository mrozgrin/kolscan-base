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
 * Retorna o leaderboard dos KOLs para um período específico
 */
export async function getLeaderboardHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const period = (req.query.period as 'daily' | 'weekly' | 'monthly' | 'all_time') || 'daily';
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const page = Math.max(parseInt(req.query.page as string) || 1, 1);
    const offset = (page - 1) * limit;

    const cacheKey = `${period}_${limit}_${offset}`;
    const cacheTtlMs = config.cache.leaderboardTtl * 1000;

    // Verificar cache
    const cached = leaderboardCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < cacheTtlMs) {
      const response: ApiResponse<LeaderboardEntry[]> = {
        success: true,
        data: cached.data,
        pagination: {
          page,
          limit,
          total: cached.data.length,
          total_pages: Math.ceil(cached.data.length / limit),
        },
      };
      res.json(response);
      return;
    }

    const leaderboard = await getLeaderboard(period, limit, offset);

    // Atualizar cache
    leaderboardCache.set(cacheKey, {
      data: leaderboard,
      timestamp: Date.now(),
    });

    const response: ApiResponse<LeaderboardEntry[]> = {
      success: true,
      data: leaderboard,
      pagination: {
        page,
        limit,
        total: leaderboard.length,
        total_pages: Math.ceil(leaderboard.length / limit),
      },
    };

    res.json(response);
  } catch (error) {
    logger.error('Error in getLeaderboard', { error: (error as Error).message });
    next(error);
  }
}
