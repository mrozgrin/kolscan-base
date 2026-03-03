import { Request, Response, NextFunction } from 'express';
import { getLeaderboard } from '../../services/metrics-service';
import { query } from '../../database/connection';
import { ApiResponse, LeaderboardEntry } from '../../types';
import { logger } from '../../utils/logger';
import { config } from '../../config';

// Cache simples em memória
const leaderboardCache = new Map<string, { data: unknown; timestamp: number }>();
const CACHE_TTL_MS = (config.cache?.leaderboardTtl ?? 60) * 1000;

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/leaderboard
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Retorna o leaderboard dos KOLs qualificados para um período.
 *
 * Query params:
 *   period   — daily | weekly | monthly | all_time  (default: monthly)
 *   sort_by  — follow_score | profit_usd | win_rate | profit_pct  (default: follow_score)
 *   limit    — 1-200  (default: 50)
 *   page     — página  (default: 1)
 */
export async function getLeaderboardHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const period  = (['daily','weekly','monthly','all_time'].includes(req.query.period as string)
      ? req.query.period as string : 'monthly');
    const sortBy  = (req.query.sort_by as string) || 'follow_score';
    const limit   = Math.min(Math.max(parseInt(req.query.limit as string) || 50, 1), 200);
    const page    = Math.max(parseInt(req.query.page as string) || 1, 1);
    const offset  = (page - 1) * limit;

    const cacheKey = `lb_${period}_${sortBy}_${limit}_${offset}`;
    const cached = leaderboardCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
      res.json(cached.data);
      return;
    }

    const leaderboard = await getLeaderboard(period, limit, offset, sortBy);

    const response = {
      success: true,
      data: leaderboard,
      meta: buildMeta(period, sortBy),
      pagination: { page, limit, count: leaderboard.length },
    };

    leaderboardCache.set(cacheKey, { data: response, timestamp: Date.now() });
    res.json(response);
  } catch (error) {
    logger.error('Error getting leaderboard', {
      period: req.query.period, error: (error as Error).message,
    });
    next(error);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/leaderboard/disqualified
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Retorna as wallets desqualificadas com o motivo da desqualificação.
 *
 * Query params:
 *   reason  — SCALPER | BUNDLER | CREATOR_FUNDED | SYBIL  (opcional, filtra por motivo)
 *   limit   — 1-200  (default: 50)
 *   page    — página  (default: 1)
 */
export async function getDisqualifiedHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const reason = (req.query.reason as string || '').toUpperCase();
    const limit  = Math.min(Math.max(parseInt(req.query.limit as string) || 50, 1), 200);
    const page   = Math.max(parseInt(req.query.page as string) || 1, 1);
    const offset = (page - 1) * limit;

    const validReasons = ['SCALPER','BUNDLER','CREATOR_FUNDED','SYBIL'];

    let whereClause = 'WHERE is_disqualified = 1';
    const params: (string | number | boolean | Date | null | undefined)[] = [];

    if (reason && validReasons.includes(reason)) {
      const colMap: Record<string,string> = {
        SCALPER: 'flag_scalper', BUNDLER: 'flag_bundler',
        CREATOR_FUNDED: 'flag_creator_funded', SYBIL: 'flag_sybil',
      };
      whereClause += ` AND ${colMap[reason]} = 1`;
    }

    const rows = await query<{
      address: string; label: string|null; first_seen: Date; last_seen: Date;
      flag_scalper: number; flag_bundler: number; flag_creator_funded: number; flag_sybil: number;
      scalper_copiability_index: number|null; flags_updated_at: Date|null;
    }>(
      `SELECT address, label, first_seen, last_seen,
              COALESCE(flag_scalper,0) AS flag_scalper,
              COALESCE(flag_bundler,0) AS flag_bundler,
              COALESCE(flag_creator_funded,0) AS flag_creator_funded,
              COALESCE(flag_sybil,0) AS flag_sybil,
              scalper_copiability_index, flags_updated_at
       FROM wallets ${whereClause}
       ORDER BY flags_updated_at DESC
       LIMIT ${limit} OFFSET ${offset}`,
      params
    );

    const data = rows.map((r) => ({
      wallet_address: r.address,
      label: r.label,
      first_seen: r.first_seen,
      last_seen: r.last_seen,
      disqualification_reason:
        r.flag_scalper        ? 'SCALPER' :
        r.flag_bundler        ? 'BUNDLER' :
        r.flag_creator_funded ? 'CREATOR_FUNDED' :
        r.flag_sybil          ? 'SYBIL' : 'UNKNOWN',
      flags: {
        scalper:        r.flag_scalper === 1,
        bundler:        r.flag_bundler === 1,
        creator_funded: r.flag_creator_funded === 1,
        sybil:          r.flag_sybil === 1,
      },
      copiability_index: r.scalper_copiability_index !== null ? Number(r.scalper_copiability_index) : null,
      copiability_pct:   r.scalper_copiability_index !== null
        ? Math.round(Number(r.scalper_copiability_index) * 10000) / 100 : null,
      flags_updated_at: r.flags_updated_at,
    }));

    res.json({
      success: true,
      data,
      pagination: { page, limit, count: data.length },
    });
  } catch (error) {
    logger.error('Error getting disqualified wallets', { error: (error as Error).message });
    next(error);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Meta
// ─────────────────────────────────────────────────────────────────────────────

function buildMeta(period: string, sortBy: string) {
  return {
    period,
    sort_by: sortBy,
    scalping_threshold_s: config.indexer.scalpingThresholdSeconds,
    follow_score_info: {
      description: 'Nota 0-100 que indica o quanto vale a pena seguir o trader',
      version: '2.0',
      components: {
        followability: { weight: '40%', sub: 'Hold Time (50%) + Volume (30%) + Liquidity (20%)' },
        consistency:   { weight: '25%', sub: 'WR Stability (40%) + PnL Stability (35%) + Diversification (25%)' },
        pnl:           { weight: '20%', sub: 'PnL vs P90 (60%) + Profit Factor (40%)' },
        win_rate:      { weight: '15%', sub: 'Curva linear 0-70%+' },
      },
      formula: 'follow_score = (followability × 0.40) + (consistency × 0.25) + (pnl × 0.20) + (win_rate × 0.15)',
      disqualification_flags: ['SCALPER','BUNDLER','CREATOR_FUNDED','SYBIL'],
    },
  };
}
