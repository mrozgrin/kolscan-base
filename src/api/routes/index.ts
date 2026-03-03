import { Router } from 'express';
import { getLeaderboardHandler, getDisqualifiedHandler } from '../controllers/leaderboard-controller';
import {
  getKolDetailsHandler,
  getKolTransactionsHandler,
  getKolSwapsHandler,
  getPlatformStatsHandler,
  searchWalletHandler,
} from '../controllers/kol-controller';
import {
  getTopTokensHandler,
  getTokenDetailsHandler,
  getTokenTradersHandler,
  getTokenPriceHistoryHandler,
  getKolBuyingNowHandler,
} from '../controllers/token-controller';
import {
  getIndexerStatusHandler,
  healthCheckHandler,
} from '../controllers/indexer-controller';
import { validateAddress, validatePeriod } from '../middleware/error-handler';

const router = Router();

// ── Health check ──────────────────────────────────────────────────────────────
router.get('/health', healthCheckHandler);

// ── Leaderboard ───────────────────────────────────────────────────────────────
router.get('/leaderboard', validatePeriod, getLeaderboardHandler);
router.get('/leaderboard/disqualified', getDisqualifiedHandler);

// ── KOL endpoints ─────────────────────────────────────────────────────────────
router.get('/kol/:address', validateAddress, getKolDetailsHandler);
router.get('/kol/:address/transactions', validateAddress, getKolTransactionsHandler);
router.get('/kol/:address/swaps', validateAddress, getKolSwapsHandler);

// ── Estatísticas gerais ───────────────────────────────────────────────────────
router.get('/stats', getPlatformStatsHandler);

// ── Busca ─────────────────────────────────────────────────────────────────────
router.get('/search', searchWalletHandler);

// ── Tokens ────────────────────────────────────────────────────────────────────
// IMPORTANTE: rotas estáticas ANTES das rotas com parâmetro (:address)
router.get('/tokens/kol-buying', getKolBuyingNowHandler);
router.get('/tokens', getTopTokensHandler);
router.get('/tokens/:address', getTokenDetailsHandler);
router.get('/tokens/:address/traders', getTokenTradersHandler);
router.get('/tokens/:address/history', getTokenPriceHistoryHandler);

// ── Indexer status ────────────────────────────────────────────────────────────
router.get('/indexer/status', getIndexerStatusHandler);

export default router;
