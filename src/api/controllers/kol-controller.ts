import { Request, Response, NextFunction } from 'express';
import { getKolDetails, getPlatformStats } from '../../services/metrics-service';
import { query } from '../../database/connection';
import { ApiResponse } from '../../types';
import { AppError } from '../middleware/error-handler';
import { config } from '../../config';

// Cache para detalhes de KOLs
const kolDetailsCache = new Map<string, { data: unknown; timestamp: number }>();

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

    const cached = kolDetailsCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < cacheTtlMs) {
      res.json({ success: true, data: cached.data });
      return;
    }

    const details = await getKolDetails(address);

    if (!details) {
      throw new AppError(`KOL with address ${address} not found`, 404);
    }

    kolDetailsCache.set(cacheKey, { data: details, timestamp: Date.now() });

    res.json({ success: true, data: details } as ApiResponse<typeof details>);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/kol/:address/transactions
 * Retorna o histórico de transações de um KOL
 *
 * Nota: LIMIT e OFFSET são interpolados diretamente na SQL porque o mysql2
 * não suporta esses valores como parâmetros de prepared statements.
 * Os valores são sanitizados via parseInt + Math.min/max antes da interpolação.
 */
export async function getKolTransactionsHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const address = (req.params.address as string).toLowerCase();
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const page = Math.max(parseInt(req.query.page as string) || 1, 1);
    const offset = (page - 1) * limit;
    const period = req.query.period as string;

    let dateFilter = '';
    if (period === 'daily') {
      dateFilter = 'AND se.timestamp >= NOW() - INTERVAL 24 HOUR';
    } else if (period === 'weekly') {
      dateFilter = 'AND se.timestamp >= NOW() - INTERVAL 7 DAY';
    } else if (period === 'monthly') {
      dateFilter = 'AND se.timestamp >= NOW() - INTERVAL 30 DAY';
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
      `SELECT tx_hash, timestamp, dex_name,
         token_in_symbol, token_in_amount,
         token_out_symbol, token_out_amount,
         value_usd, pnl, is_win
       FROM swap_events se
       WHERE wallet_address = ?
       ${dateFilter}
       ORDER BY timestamp DESC
       LIMIT ${limit} OFFSET ${offset}`,
      [address]
    );

    const countResult = await query<{ total: number }>(
      `SELECT COUNT(*) AS total FROM swap_events se WHERE wallet_address = ? ${dateFilter}`,
      [address]
    );

    const total = Number(countResult[0]?.total) || 0;

    res.json({
      success: true,
      data: transactions,
      pagination: { page, limit, total, total_pages: Math.ceil(total / limit) },
    } as ApiResponse<typeof transactions>);
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
    const address = (req.params.address as string).toLowerCase();
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
       WHERE wallet_address = ?
       ORDER BY timestamp DESC
       LIMIT ${limit} OFFSET ${offset}`,
      [address]
    );

    const countResult = await query<{ total: number }>(
      'SELECT COUNT(*) AS total FROM swap_events WHERE wallet_address = ?',
      [address]
    );

    const total = Number(countResult[0]?.total) || 0;

    res.json({
      success: true,
      data: swaps,
      pagination: { page, limit, total, total_pages: Math.ceil(total / limit) },
    } as ApiResponse<typeof swaps>);
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
    res.json({ success: true, data: stats } as ApiResponse<typeof stats>);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/search
 * Busca uma carteira pelo endereço ou label
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

    const searchTerm = `%${q.toLowerCase().trim()}%`;

    const results = await query<{
      address: string;
      label: string | null;
      total_transactions: number;
      last_seen: Date;
    }>(
      `SELECT address, label, total_transactions, last_seen
       FROM wallets
       WHERE address LIKE ? OR label LIKE ?
       ORDER BY total_transactions DESC
       LIMIT 10`,
      [searchTerm, searchTerm]
    );

    res.json({ success: true, data: results } as ApiResponse<typeof results>);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/tokens
 * Retorna os tokens mais negociados nas últimas 24h
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
      swap_count: number;
      unique_traders: number;
      total_volume_usd: number;
    }>(
      `SELECT
         token_out_address                    AS token_address,
         token_out_symbol                     AS token_symbol,
         COUNT(*)                             AS swap_count,
         COUNT(DISTINCT wallet_address)       AS unique_traders,
         COALESCE(SUM(ABS(value_usd)), 0)     AS total_volume_usd
       FROM swap_events
       WHERE timestamp >= NOW() - INTERVAL 24 HOUR
         AND token_out_symbol != 'UNKNOWN'
       GROUP BY token_out_address, token_out_symbol
       ORDER BY COUNT(*) DESC
       LIMIT ${limit}`,
      []
    );

    res.json({ success: true, data: tokens } as ApiResponse<typeof tokens>);
  } catch (error) {
    next(error);
  }
}
