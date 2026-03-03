import { Request, Response, NextFunction } from 'express';
import {
  getTokenDetails,
  getTokenStats,
  getTokenTopTraders,
  getTopTokens,
  getKolBuyingNow,
  getTokenPriceHistory,
} from '../../services/token-service';
import { ApiResponse } from '../../types';
import { AppError } from '../middleware/error-handler';

// Cache em memória para top tokens (atualiza a cada 2 min)
const topTokensCache = new Map<string, { data: unknown; timestamp: number }>();
const TOP_TOKENS_CACHE_TTL_MS = 2 * 60 * 1000;

const kolBuyingCache = { data: null as unknown, timestamp: 0 };
const KOL_BUYING_CACHE_TTL_MS = 60 * 1000; // 1 min

/**
 * GET /api/tokens
 * Retorna os tokens mais negociados com métricas on-chain e market data
 *
 * Query params:
 *   period    = daily | weekly | monthly  (padrão: daily)
 *   sort_by   = volume | traders | swaps | pnl  (padrão: volume)
 *   limit     = 1-100  (padrão: 20)
 *   kol_only  = true | false  (padrão: false)
 */
export async function getTopTokensHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const period = (['daily', 'weekly', 'monthly'].includes(req.query.period as string)
      ? req.query.period
      : 'daily') as 'daily' | 'weekly' | 'monthly';

    const sort_by = (['volume', 'traders', 'swaps', 'pnl'].includes(req.query.sort_by as string)
      ? req.query.sort_by
      : 'volume') as 'volume' | 'traders' | 'swaps' | 'pnl';

    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);
    const kol_only = req.query.kol_only === 'true';

    const cacheKey = `${period}:${sort_by}:${limit}:${kol_only}`;
    const cached = topTokensCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < TOP_TOKENS_CACHE_TTL_MS) {
      res.json({ success: true, data: cached.data, meta: { period, sort_by, kol_only } });
      return;
    }

    const tokens = await getTopTokens({ period, sort_by, limit, only_kol_tokens: kol_only });
    topTokensCache.set(cacheKey, { data: tokens, timestamp: Date.now() });

    res.json({
      success: true,
      data: tokens,
      meta: { period, sort_by, limit, kol_only },
    } as ApiResponse<typeof tokens>);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/tokens/:address
 * Retorna detalhes completos de um token específico
 */
export async function getTokenDetailsHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const address = (req.params.address as string).toLowerCase();

    if (!/^0x[0-9a-f]{40}$/i.test(address)) {
      throw new AppError('Invalid token address', 400);
    }

    const stats = await getTokenStats(address);

    if (!stats) {
      throw new AppError(`Token ${address} not found`, 404);
    }

    res.json({ success: true, data: stats } as ApiResponse<typeof stats>);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/tokens/:address/traders
 * Retorna os top traders de um token específico
 *
 * Query params:
 *   period  = daily | weekly | monthly | all_time  (padrão: weekly)
 *   limit   = 1-50  (padrão: 20)
 */
export async function getTokenTradersHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const address = (req.params.address as string).toLowerCase();

    if (!/^0x[0-9a-f]{40}$/i.test(address)) {
      throw new AppError('Invalid token address', 400);
    }

    const period = (['daily', 'weekly', 'monthly', 'all_time'].includes(
      req.query.period as string
    )
      ? req.query.period
      : 'weekly') as 'daily' | 'weekly' | 'monthly' | 'all_time';

    const limit = Math.min(parseInt(req.query.limit as string) || 20, 50);

    const traders = await getTokenTopTraders(address, limit, period);

    res.json({
      success: true,
      data: traders,
      meta: { token_address: address, period, limit },
    } as ApiResponse<typeof traders>);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/tokens/:address/history
 * Retorna o histórico de preço on-chain de um token
 *
 * Query params:
 *   period  = daily | weekly | monthly  (padrão: weekly)
 */
export async function getTokenPriceHistoryHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const address = (req.params.address as string).toLowerCase();

    if (!/^0x[0-9a-f]{40}$/i.test(address)) {
      throw new AppError('Invalid token address', 400);
    }

    const period = (['daily', 'weekly', 'monthly'].includes(req.query.period as string)
      ? req.query.period
      : 'weekly') as 'daily' | 'weekly' | 'monthly';

    const history = await getTokenPriceHistory(address, period);

    res.json({
      success: true,
      data: history,
      meta: { token_address: address, period },
    } as ApiResponse<typeof history>);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/tokens/kol-buying
 * Retorna tokens que múltiplos KOLs qualificados estão comprando agora (últimas 2h)
 * Útil para detectar movimentos coordenados de KOLs
 */
export async function getKolBuyingNowHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 20, 50);

    // Cache de 1 minuto para esse endpoint (muito consultado)
    if (kolBuyingCache.data && Date.now() - kolBuyingCache.timestamp < KOL_BUYING_CACHE_TTL_MS) {
      res.json({ success: true, data: kolBuyingCache.data });
      return;
    }

    const tokens = await getKolBuyingNow(limit);
    kolBuyingCache.data = tokens;
    kolBuyingCache.timestamp = Date.now();

    res.json({
      success: true,
      data: tokens,
      meta: {
        description: 'Tokens sendo comprados por 2+ KOLs qualificados nas últimas 2 horas',
        window_hours: 2,
        min_kol_count: 2,
      },
    } as ApiResponse<typeof tokens>);
  } catch (error) {
    next(error);
  }
}
