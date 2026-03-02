import { Request, Response, NextFunction } from 'express';
import { logger } from '../../utils/logger';

export function requestLogger(
  req: Request,
  res: Response,
  next: NextFunction
): void {
  const start = Date.now();

  res.on('finish', () => {
    const duration = Date.now() - start;

    logger.info('HTTP Request', {
      method: req.method,
      url: req.url,
      statusCode: res.statusCode,
      duration,
      ip: req.ip,
      userAgent: req.get('user-agent'),
    });
  });

  next();
}

export function rateLimiter(
  maxRequests: number = 100,
  windowMs: number = 60000
) {
  const requests = new Map<string, { count: number; resetTime: number }>();

  return (req: Request, res: Response, next: NextFunction): void => {
    const ip = req.ip || 'unknown';
    const now = Date.now();

    const clientData = requests.get(ip);

    if (!clientData || now > clientData.resetTime) {
      requests.set(ip, { count: 1, resetTime: now + windowMs });
      next();
      return;
    }

    if (clientData.count >= maxRequests) {
      res.status(429).json({
        success: false,
        error: 'Too many requests. Please try again later.',
        retryAfter: Math.ceil((clientData.resetTime - now) / 1000),
      });
      return;
    }

    clientData.count++;
    next();
  };
}
