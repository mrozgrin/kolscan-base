import { Request, Response, NextFunction } from 'express';
import { getKolDetails, getPlatformStats } from '../../services/metrics-service';
import { query } from '../../database/connection';
import { ApiResponse } from '../../types';
import { logger } from '../../utils/logger';
import { AppError } from '../middleware/error-handler';
import { config } from '../../config';

// Cache para detalhes de KOLs
const kolDetailsCache = new Map<
  string,
  { data: unknown; timestamp: number }
>();

/**
 * GET /api/kol/:address
 * Retorna os detalhes completos de um KOL
 */
export async function getKolDetailsHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const address = req.params.address as string;
    const cacheKey = address.toLowerCase();
    const cacheTtlMs = config.cache.kolDetailsTtl * 1000;

    // Verificar cache
    const cached = kolDetailsCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < cacheTtlMs) {
      res.json({ success: true, data: cached.data });
      return;
    }

    const details = await getKolDetails(address as string);

    if (!details) {
      throw new AppError(`KOL with address ${address} not found`, 404);
    }

    // Atualizar cache
    kolDetailsCache.set(cacheKey, {
      data: details,
      timestamp: Date.now(),
    });

    const response: ApiResponse<typeof details> = {
      success: true,
      data: details,
    };

    res.json(response);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/kol/:address/transactions
 * Retorna o histórico de transações de um KOL
 */
export async function getKolTransactionsHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const address = req.params.address as string;
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const page = Math.max(parseInt(req.query.page as string) || 1, 1);
    const offset = (page - 1) * limit;
    const period = req.query.period as string;

    let dateFilter = '';
    const params: unknown[] = [(address as string).toLowerCase(), limit, offset];

    if (period === 'daily') {
      dateFilter = "AND se.timestamp >= NOW() - INTERVAL '24 hours'";
    } else if (period === 'weekly') {
      dateFilter = "AND se.timestamp >= NOW() - INTERVAL '7 days'";
    } else if (period === 'monthly') {
      dateFilter = "AND se.timestamp >= NOW() - INTERVAL '30 days'";
    }

    const transactions = await query<{
      tx_hash: string;
      timestamp: Date;
      dex_name: string;
      token_in_symbol: string;
      token_in_amount: string;
      token_out_symbol: string;
      token_out_amount: string;
      value_usd: number | null;
      pnl: number | null;
      is_win: boolean | null;
    }>(
      `SELECT
         tx_hash, timestamp, dex_name,
         token_in_symbol, token_in_amount,
         token_out_symbol, token_out_amount,
         value_usd, pnl, is_win
       FROM swap_events se
       WHERE wallet_address = $1
       ${dateFilter}
       ORDER BY timestamp DESC
       LIMIT $2 OFFSET $3`,
      params
    );

    // Contar total
    const countResult = await query<{ count: string }>(
      `SELECT COUNT(*)::TEXT as count FROM swap_events se WHERE wallet_address = $1 ${dateFilter}`,
      [(address as string).toLowerCase()]
    );

    const total = parseInt(countResult[0]?.count || '0');

    const response: ApiResponse<typeof transactions> = {
      success: true,
      data: transactions,
      pagination: {
        page,
        limit,
        total,
        total_pages: Math.ceil(total / limit),
      },
    };

    res.json(response);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/kol/:address/swaps
 * Retorna os swaps de um KOL com detalhes completos
 */
export async function getKolSwapsHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const address = req.params.address as string;
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const page = Math.max(parseInt(req.query.page as string) || 1, 1);
    const offset = (page - 1) * limit;

    const swaps = await query<{
      id: number;
      tx_hash: string;
      timestamp: Date;
      dex_name: string;
      token_in_address: string;
      token_in_symbol: string;
      token_in_amount: string;
      token_out_address: string;
      token_out_symbol: string;
      token_out_amount: string;
      value_usd: number | null;
      pnl: number | null;
      is_win: boolean | null;
    }>(
      `SELECT id, tx_hash, timestamp, dex_name,
         token_in_address, token_in_symbol, token_in_amount,
         token_out_address, token_out_symbol, token_out_amount,
         value_usd, pnl, is_win
       FROM swap_events
       WHERE wallet_address = $1
       ORDER BY timestamp DESC
       LIMIT $2 OFFSET $3`,
      [(address as string).toLowerCase(), limit, offset]
    );

    const countResult = await query<{ count: string }>(
      'SELECT COUNT(*)::TEXT as count FROM swap_events WHERE wallet_address = $1',
      [(address as string).toLowerCase()]
    );

    const total = parseInt(countResult[0]?.count || '0');

    const response: ApiResponse<typeof swaps> = {
      success: true,
      data: swaps,
      pagination: {
        page,
        limit,
        total,
        total_pages: Math.ceil(total / limit),
      },
    };

    res.json(response);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/stats
 * Retorna estatísticas gerais da plataforma
 */
export async function getPlatformStatsHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const stats = await getPlatformStats();

    const response: ApiResponse<typeof stats> = {
      success: true,
      data: stats,
    };

    res.json(response);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/search
 * Busca uma carteira pelo endereço
 */
export async function searchWalletHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const { q } = req.query;

    if (!q || typeof q !== 'string') {
      throw new AppError('Query parameter "q" is required', 400);
    }

    const searchTerm = q.toLowerCase().trim();

    // Buscar por endereço exato ou label
    const results = await query<{
      address: string;
      label: string | null;
      total_transactions: number;
      last_seen: Date;
    }>(
      `SELECT address, label, total_transactions, last_seen
       FROM wallets
       WHERE address ILIKE $1 OR label ILIKE $1
       ORDER BY total_transactions DESC
       LIMIT 10`,
      [`%${searchTerm}%`]
    );

    const response: ApiResponse<typeof results> = {
      success: true,
      data: results,
    };

    res.json(response);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/tokens
 * Retorna os tokens mais negociados
 */
export async function getTopTokensHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);

    const tokens = await query<{
      token_address: string;
      token_symbol: string;
      swap_count: string;
      unique_traders: string;
      total_volume_usd: string;
    }>(
      `SELECT
         token_out_address as token_address,
         token_out_symbol as token_symbol,
         COUNT(*)::TEXT as swap_count,
         COUNT(DISTINCT wallet_address)::TEXT as unique_traders,
         COALESCE(SUM(ABS(value_usd)), 0)::TEXT as total_volume_usd
       FROM swap_events
       WHERE timestamp >= NOW() - INTERVAL '24 hours'
         AND token_out_symbol != 'UNKNOWN'
       GROUP BY token_out_address, token_out_symbol
       ORDER BY COUNT(*) DESC
       LIMIT $1`,
      [limit]
    );

    const response: ApiResponse<typeof tokens> = {
      success: true,
      data: tokens,
    };

    res.json(response);
  } catch (error) {
    next(error);
  }
}
