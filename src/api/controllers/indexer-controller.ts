import { Request, Response, NextFunction } from 'express';
import { getIndexerStatus } from '../../indexer/block-indexer';
import { query } from '../../database/connection';
import { ApiResponse } from '../../types';
import { logger } from '../../utils/logger';

/**
 * GET /api/indexer/status
 * Retorna o status atual do indexador
 */
export async function getIndexerStatusHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const status = await getIndexerStatus();

    const dbState = await query<{
      last_indexed_block: number;
      last_updated: Date;
      is_syncing: boolean;
      sync_progress: number;
    }>('SELECT last_indexed_block, last_updated, is_syncing, sync_progress FROM indexer_state WHERE id = 1');

    const response: ApiResponse<{
      runtime: typeof status;
      database: typeof dbState[0];
    }> = {
      success: true,
      data: {
        runtime: status,
        database: dbState[0] || null,
      },
    };

    res.json(response);
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/health
 * Health check endpoint
 */
export async function healthCheckHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    // Verificar conexão com banco de dados
    const dbCheck = await query<{ now: Date }>('SELECT NOW()');
    const dbOk = dbCheck.length > 0;

    const response: ApiResponse<{
      status: string;
      database: boolean;
      timestamp: string;
      version: string;
    }> = {
      success: true,
      data: {
        status: dbOk ? 'healthy' : 'degraded',
        database: dbOk,
        timestamp: new Date().toISOString(),
        version: process.env.npm_package_version || '1.0.0',
      },
    };

    res.status(dbOk ? 200 : 503).json(response);
  } catch (error) {
    res.status(503).json({
      success: false,
      data: {
        status: 'unhealthy',
        database: false,
        timestamp: new Date().toISOString(),
      },
    });
  }
}
