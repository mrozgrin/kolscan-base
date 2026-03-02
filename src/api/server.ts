import express from 'express';
import cors from 'cors';
import { config } from '../config';
import { logger } from '../utils/logger';
import routes from './routes';
import { errorHandler, notFoundHandler } from './middleware/error-handler';
import { requestLogger, rateLimiter } from './middleware/request-logger';

export function createServer(): express.Application {
  const app = express();

  // Middlewares globais
  app.use(cors({
    origin: process.env.CORS_ORIGIN || '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
  }));

  app.use(express.json({ limit: '10mb' }));
  app.use(express.urlencoded({ extended: true }));

  // Logging de requisições
  app.use(requestLogger);

  // Rate limiting (100 req/min por IP)
  app.use('/api', rateLimiter(100, 60000));

  // Rotas da API
  app.use('/api', routes);

  // Rota raiz com informações da API
  app.get('/', (req, res) => {
    res.json({
      name: 'KOLScan Base API',
      description: 'Backend para rastreamento de KOLs na blockchain Base',
      version: process.env.npm_package_version || '1.0.0',
      endpoints: {
        health: 'GET /api/health',
        leaderboard: 'GET /api/leaderboard?period=daily|weekly|monthly|all_time',
        kol_details: 'GET /api/kol/:address',
        kol_transactions: 'GET /api/kol/:address/transactions',
        kol_swaps: 'GET /api/kol/:address/swaps',
        stats: 'GET /api/stats',
        search: 'GET /api/search?q=:query',
        tokens: 'GET /api/tokens',
        indexer_status: 'GET /api/indexer/status',
      },
      documentation: 'https://github.com/mrozgrin/kolscan-base#readme',
    });
  });

  // Handler para rotas não encontradas
  app.use(notFoundHandler);

  // Handler global de erros
  app.use(errorHandler);

  return app;
}

export async function startServer(): Promise<void> {
  const app = createServer();

  const server = app.listen(config.port, () => {
    logger.info(`KOLScan Base API started`, {
      port: config.port,
      env: config.nodeEnv,
      blockchain: config.blockchain.chainName,
      chainId: config.blockchain.chainId,
    });
  });

  // Graceful shutdown
  const shutdown = async (signal: string): Promise<void> => {
    logger.info(`Received ${signal}, shutting down gracefully...`);

    server.close(async () => {
      logger.info('HTTP server closed');
      process.exit(0);
    });

    // Force close after 10 seconds
    setTimeout(() => {
      logger.error('Could not close connections in time, forcefully shutting down');
      process.exit(1);
    }, 10000);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}
