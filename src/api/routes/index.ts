import { Router } from 'express';
import { getLeaderboardHandler, getDisqualifiedHandler } from '../controllers/leaderboard-controller';
import {
  getKolDetailsHandler,
  getKolTransactionsHandler,
  getKolSwapsHandler,
  getPlatformStatsHandler,
  searchWalletHandler,
  getTopTokensHandler,
} from '../controllers/kol-controller';
import {
  getIndexerStatusHandler,
  healthCheckHandler,
} from '../controllers/indexer-controller';
import { validateAddress, validatePeriod } from '../middleware/error-handler';

const router = Router();

// Health check
router.get('/health', healthCheckHandler);

// Leaderboard
router.get('/leaderboard', validatePeriod, getLeaderboardHandler);

// Wallets desqualificadas
router.get('/leaderboard/disqualified', getDisqualifiedHandler);

// KOL endpoints
router.get('/kol/:address', validateAddress, getKolDetailsHandler);
router.get('/kol/:address/transactions', validateAddress, getKolTransactionsHandler);
router.get('/kol/:address/swaps', validateAddress, getKolSwapsHandler);

// Estatísticas gerais
router.get('/stats', getPlatformStatsHandler);

// Busca
router.get('/search', searchWalletHandler);

// Tokens
router.get('/tokens', getTopTokensHandler);

// Indexer status
router.get('/indexer/status', getIndexerStatusHandler);

export default router;
