import { Request, Response, NextFunction } from 'express';
import { logger } from '../../utils/logger';
import { ApiResponse } from '../../types';

export class AppError extends Error {
  public statusCode: number;
  public isOperational: boolean;

  constructor(message: string, statusCode: number = 500) {
    super(message);
    this.statusCode = statusCode;
    this.isOperational = true;
    Error.captureStackTrace(this, this.constructor);
  }
}

export function errorHandler(
  err: Error | AppError,
  req: Request,
  res: Response,
  _next: NextFunction
): void {
  const statusCode = 'statusCode' in err ? err.statusCode : 500;
  const message = err.message || 'Internal Server Error';

  logger.error('Request error', {
    method: req.method,
    url: req.url,
    statusCode,
    error: message,
    stack: process.env.NODE_ENV === 'development' ? err.stack : undefined,
  });

  const response: ApiResponse<null> = {
    success: false,
    error: message,
  };

  res.status(statusCode).json(response);
}

export function notFoundHandler(req: Request, res: Response): void {
  const response: ApiResponse<null> = {
    success: false,
    error: `Route ${req.method} ${req.url} not found`,
  };

  res.status(404).json(response);
}

export function validateAddress(
  req: Request,
  res: Response,
  next: NextFunction
): void {
  const address = req.params.address as string;

  if (!address || !/^0x[a-fA-F0-9]{40}$/.test(address)) {
    res.status(400).json({
      success: false,
      error: 'Invalid Ethereum address format',
    } as ApiResponse<null>);
    return;
  }

  next();
}

export function validatePeriod(
  req: Request,
  res: Response,
  next: NextFunction
): void {
  const { period } = req.query;
  const validPeriods = ['daily', 'weekly', 'monthly', 'all_time'];

  if (period && !validPeriods.includes(period as string)) {
    res.status(400).json({
      success: false,
      error: `Invalid period. Must be one of: ${validPeriods.join(', ')}`,
    } as ApiResponse<null>);
    return;
  }

  next();
}
