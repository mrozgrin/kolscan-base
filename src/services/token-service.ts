import axios from 'axios';
import { query, execute, ddl } from '../database/connection';
import { logger } from '../utils/logger';
import { getTokenPriceFromDexScreener } from './price-service';

const DEXSCREENER_BASE_URL = 'https://api.dexscreener.com/latest/dex';

// ─────────────────────────────────────────────────────────────────────────────
// Tipos
// ─────────────────────────────────────────────────────────────────────────────

export interface TokenInfo {
  address: string;
  symbol: string | null;
  name: string | null;
  decimals: number;
  price_usd: number | null;
  price_change_24h: number | null;
  market_cap_usd: number | null;
  liquidity_usd: number | null;
  volume_24h_usd: number | null;
  fdv_usd: number | null;
  pair_address: string | null;
  dex_id: string | null;
  deployer_address: string | null;
  deploy_block: number | null;
  deploy_timestamp: Date | null;
  is_verified: boolean;
  price_updated_at: Date | null;
}

export interface TokenStats {
  address: string;
  symbol: string | null;
  name: string | null;
  price_usd: number | null;
  price_change_24h: number | null;
  market_cap_usd: number | null;
  liquidity_usd: number | null;
  volume_24h_usd: number | null;
  // Métricas on-chain calculadas pelo indexador
  swap_count_24h: number;
  swap_count_7d: number;
  unique_traders_24h: number;
  unique_traders_7d: number;
  total_volume_usd_24h: number;
  total_volume_usd_7d: number;
  buy_count_24h: number;
  sell_count_24h: number;
  avg_trade_size_usd: number | null;
  top_kol_count: number; // quantos KOLs qualificados negociaram esse token
}

export interface TokenTopTrader {
  wallet_address: string;
  label: string | null;
  follow_score: number | null;
  trade_count: number;
  volume_usd: number;
  pnl_usd: number;
  win_rate: number;
  first_trade: Date;
  last_trade: Date;
}

// ─────────────────────────────────────────────────────────────────────────────
// Cache em memória para market data
// ─────────────────────────────────────────────────────────────────────────────
const marketDataCache = new Map<string, { data: DexScreenerPairData; timestamp: number }>();
const MARKET_CACHE_TTL_MS = 5 * 60 * 1000; // 5 min

interface DexScreenerPairData {
  priceUsd: string;
  priceChange?: { h24?: number };
  liquidity?: { usd?: number };
  volume?: { h24?: number };
  fdv?: number;
  marketCap?: number;
  pairAddress?: string;
  dexId?: string;
  baseToken?: { address: string; symbol: string; name: string };
  quoteToken?: { address: string; symbol: string; name: string };
}

// ─────────────────────────────────────────────────────────────────────────────
// Funções internas
// ─────────────────────────────────────────────────────────────────────────────

async function fetchDexScreenerData(tokenAddress: string): Promise<DexScreenerPairData | null> {
  const cached = marketDataCache.get(tokenAddress);
  if (cached && Date.now() - cached.timestamp < MARKET_CACHE_TTL_MS) {
    return cached.data;
  }

  try {
    const url = `${DEXSCREENER_BASE_URL}/tokens/${tokenAddress}`;
    const response = await axios.get(url, { timeout: 8000 });

    if (!response.data?.pairs?.length) return null;

    const basePairs: DexScreenerPairData[] = response.data.pairs
      .filter((p: { chainId: string }) => p.chainId === 'base')
      .sort(
        (a: { liquidity?: { usd?: number } }, b: { liquidity?: { usd?: number } }) =>
          (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0)
      );

    if (!basePairs.length) return null;

    const best = basePairs[0];
    marketDataCache.set(tokenAddress, { data: best, timestamp: Date.now() });
    return best;
  } catch {
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// API pública
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Retorna informações completas de um token combinando banco + DexScreener
 */
export async function getTokenDetails(tokenAddress: string): Promise<TokenInfo | null> {
  const addr = tokenAddress.toLowerCase();

  const dbRows = await query<{
    address: string;
    symbol: string | null;
    name: string | null;
    decimals: number;
    price_usd: number | null;
    price_updated_at: Date | null;
    is_verified: number;
    deployer_address: string | null;
    deploy_block: number | null;
    deploy_timestamp: Date | null;
  }>(
    `SELECT address, symbol, name, decimals, price_usd, price_updated_at,
            is_verified, deployer_address, deploy_block, deploy_timestamp
     FROM tokens WHERE address = ?`,
    [addr]
  );

  const dbToken = dbRows[0] || null;

  // Buscar market data do DexScreener
  const dex = await fetchDexScreenerData(addr);

  if (!dbToken && !dex) return null;

  // Mesclar dados
  const info: TokenInfo = {
    address: addr,
    symbol: dbToken?.symbol || dex?.baseToken?.symbol || null,
    name: dbToken?.name || dex?.baseToken?.name || null,
    decimals: dbToken?.decimals ?? 18,
    price_usd: dex?.priceUsd ? parseFloat(dex.priceUsd) : (dbToken?.price_usd ?? null),
    price_change_24h: dex?.priceChange?.h24 ?? null,
    market_cap_usd: dex?.marketCap ?? null,
    liquidity_usd: dex?.liquidity?.usd ?? null,
    volume_24h_usd: dex?.volume?.h24 ?? null,
    fdv_usd: dex?.fdv ?? null,
    pair_address: dex?.pairAddress ?? null,
    dex_id: dex?.dexId ?? null,
    deployer_address: dbToken?.deployer_address ?? null,
    deploy_block: dbToken?.deploy_block ?? null,
    deploy_timestamp: dbToken?.deploy_timestamp ?? null,
    is_verified: Boolean(dbToken?.is_verified),
    price_updated_at: dbToken?.price_updated_at ?? null,
  };

  // Atualizar banco com dados frescos do DexScreener
  if (dex) {
    try {
      await execute(
        `INSERT INTO tokens (address, symbol, name, decimals, price_usd, price_updated_at)
         VALUES (?, ?, ?, ?, ?, NOW())
         ON DUPLICATE KEY UPDATE
           symbol = COALESCE(VALUES(symbol), symbol),
           name   = COALESCE(VALUES(name), name),
           price_usd = VALUES(price_usd),
           price_updated_at = NOW()`,
        [
          addr,
          info.symbol,
          info.name,
          info.decimals,
          info.price_usd,
        ]
      );
    } catch {
      // Não crítico
    }
  }

  return info;
}

/**
 * Retorna estatísticas completas de um token (market data + métricas on-chain)
 */
export async function getTokenStats(tokenAddress: string): Promise<TokenStats | null> {
  const addr = tokenAddress.toLowerCase();

  const info = await getTokenDetails(addr);
  if (!info && !(await tokenExistsInSwaps(addr))) return null;

  // Métricas on-chain das últimas 24h
  const stats24h = await query<{
    swap_count: number;
    unique_traders: number;
    total_volume_usd: number;
    buy_count: number;
    sell_count: number;
    avg_trade_size_usd: number | null;
  }>(
    `SELECT
       COUNT(*)                                                       AS swap_count,
       COUNT(DISTINCT wallet_address)                                 AS unique_traders,
       COALESCE(SUM(ABS(value_usd)), 0)                              AS total_volume_usd,
       SUM(CASE WHEN token_out_address = ? THEN 1 ELSE 0 END)        AS buy_count,
       SUM(CASE WHEN token_in_address  = ? THEN 1 ELSE 0 END)        AS sell_count,
       AVG(ABS(value_usd))                                           AS avg_trade_size_usd
     FROM swap_events
     WHERE (token_in_address = ? OR token_out_address = ?)
       AND timestamp >= NOW() - INTERVAL 24 HOUR`,
    [addr, addr, addr, addr]
  );

  // Métricas on-chain dos últimos 7 dias
  const stats7d = await query<{
    swap_count: number;
    unique_traders: number;
    total_volume_usd: number;
  }>(
    `SELECT
       COUNT(*)                           AS swap_count,
       COUNT(DISTINCT wallet_address)     AS unique_traders,
       COALESCE(SUM(ABS(value_usd)), 0)  AS total_volume_usd
     FROM swap_events
     WHERE (token_in_address = ? OR token_out_address = ?)
       AND timestamp >= NOW() - INTERVAL 7 DAY`,
    [addr, addr]
  );

  // Quantos KOLs qualificados (não desqualificados) negociaram esse token
  const kolCount = await query<{ cnt: number }>(
    `SELECT COUNT(DISTINCT se.wallet_address) AS cnt
     FROM swap_events se
     JOIN wallets w ON w.address = se.wallet_address
     WHERE (se.token_in_address = ? OR se.token_out_address = ?)
       AND w.is_disqualified = 0
       AND se.timestamp >= NOW() - INTERVAL 7 DAY`,
    [addr, addr]
  );

  const s24 = stats24h[0];
  const s7d = stats7d[0];

  return {
    address: addr,
    symbol: info?.symbol ?? null,
    name: info?.name ?? null,
    price_usd: info?.price_usd ?? null,
    price_change_24h: info?.price_change_24h ?? null,
    market_cap_usd: info?.market_cap_usd ?? null,
    liquidity_usd: info?.liquidity_usd ?? null,
    volume_24h_usd: info?.volume_24h_usd ?? null,
    swap_count_24h: Number(s24?.swap_count) || 0,
    swap_count_7d: Number(s7d?.swap_count) || 0,
    unique_traders_24h: Number(s24?.unique_traders) || 0,
    unique_traders_7d: Number(s7d?.unique_traders) || 0,
    total_volume_usd_24h: Number(s24?.total_volume_usd) || 0,
    total_volume_usd_7d: Number(s7d?.total_volume_usd) || 0,
    buy_count_24h: Number(s24?.buy_count) || 0,
    sell_count_24h: Number(s24?.sell_count) || 0,
    avg_trade_size_usd: s24?.avg_trade_size_usd ? Number(s24.avg_trade_size_usd) : null,
    top_kol_count: Number(kolCount[0]?.cnt) || 0,
  };
}

/**
 * Retorna os top traders de um token ordenados por PnL
 */
export async function getTokenTopTraders(
  tokenAddress: string,
  limit = 20,
  period: 'daily' | 'weekly' | 'monthly' | 'all_time' = 'weekly'
): Promise<TokenTopTrader[]> {
  const addr = tokenAddress.toLowerCase();

  let dateFilter = '';
  if (period === 'daily')   dateFilter = 'AND se.timestamp >= NOW() - INTERVAL 24 HOUR';
  if (period === 'weekly')  dateFilter = 'AND se.timestamp >= NOW() - INTERVAL 7 DAY';
  if (period === 'monthly') dateFilter = 'AND se.timestamp >= NOW() - INTERVAL 30 DAY';

  const rows = await query<{
    wallet_address: string;
    label: string | null;
    follow_score: number | null;
    trade_count: number;
    volume_usd: number;
    pnl_usd: number;
    wins: number;
    total: number;
    first_trade: Date;
    last_trade: Date;
  }>(
    `SELECT
       se.wallet_address,
       w.label,
       (SELECT km.follow_score
        FROM kol_metrics km
        WHERE km.wallet_address = se.wallet_address
          AND km.period = 'weekly'
        ORDER BY km.period_start DESC
        LIMIT 1)                                                    AS follow_score,
       COUNT(*)                                                     AS trade_count,
       COALESCE(SUM(ABS(se.value_usd)), 0)                         AS volume_usd,
       COALESCE(SUM(se.pnl), 0)                                    AS pnl_usd,
       SUM(CASE WHEN se.is_win = 1 THEN 1 ELSE 0 END)              AS wins,
       COUNT(*)                                                     AS total,
       MIN(se.timestamp)                                            AS first_trade,
       MAX(se.timestamp)                                            AS last_trade
     FROM swap_events se
     JOIN wallets w ON w.address = se.wallet_address
     WHERE (se.token_in_address = ? OR se.token_out_address = ?)
       AND w.is_disqualified = 0
       ${dateFilter}
     GROUP BY se.wallet_address, w.label
     ORDER BY pnl_usd DESC
     LIMIT ${limit}`,
    [addr, addr]
  );

  return rows.map((r) => ({
    wallet_address: r.wallet_address,
    label: r.label,
    follow_score: r.follow_score ? Number(r.follow_score) : null,
    trade_count: Number(r.trade_count),
    volume_usd: Number(r.volume_usd),
    pnl_usd: Number(r.pnl_usd),
    win_rate: r.total > 0 ? (Number(r.wins) / Number(r.total)) * 100 : 0,
    first_trade: r.first_trade,
    last_trade: r.last_trade,
  }));
}

/**
 * Retorna os tokens mais negociados com métricas on-chain + market data
 */
export async function getTopTokens(options: {
  period?: 'daily' | 'weekly' | 'monthly';
  sort_by?: 'volume' | 'traders' | 'swaps' | 'pnl';
  limit?: number;
  only_kol_tokens?: boolean; // apenas tokens negociados por KOLs qualificados
}): Promise<TokenStats[]> {
  const {
    period = 'daily',
    sort_by = 'volume',
    limit = 20,
    only_kol_tokens = false,
  } = options;

  let interval = '24 HOUR';
  if (period === 'weekly')  interval = '7 DAY';
  if (period === 'monthly') interval = '30 DAY';

  const kolFilter = only_kol_tokens
    ? `AND EXISTS (
         SELECT 1 FROM wallets w2
         WHERE w2.address = se.wallet_address AND w2.is_disqualified = 0
       )`
    : '';

  let orderBy = 'total_volume_usd DESC';
  if (sort_by === 'traders') orderBy = 'unique_traders DESC';
  if (sort_by === 'swaps')   orderBy = 'swap_count DESC';
  if (sort_by === 'pnl')     orderBy = 'total_pnl DESC';

  const rows = await query<{
    token_address: string;
    token_symbol: string | null;
    swap_count: number;
    unique_traders: number;
    total_volume_usd: number;
    buy_count: number;
    sell_count: number;
    total_pnl: number;
    avg_trade_size_usd: number | null;
    kol_count: number;
  }>(
    `SELECT
       token_out_address                                                AS token_address,
       MAX(token_out_symbol)                                            AS token_symbol,
       COUNT(*)                                                         AS swap_count,
       COUNT(DISTINCT se.wallet_address)                                AS unique_traders,
       COALESCE(SUM(ABS(se.value_usd)), 0)                             AS total_volume_usd,
       SUM(CASE WHEN se.token_out_address = token_out_address THEN 1 ELSE 0 END) AS buy_count,
       SUM(CASE WHEN se.token_in_address  = token_out_address THEN 1 ELSE 0 END) AS sell_count,
       COALESCE(SUM(se.pnl), 0)                                        AS total_pnl,
       AVG(ABS(se.value_usd))                                          AS avg_trade_size_usd,
       COUNT(DISTINCT CASE WHEN w.is_disqualified = 0 THEN se.wallet_address END) AS kol_count
     FROM swap_events se
     JOIN wallets w ON w.address = se.wallet_address
     WHERE se.timestamp >= NOW() - INTERVAL ${interval}
       AND se.token_out_symbol IS NOT NULL
       AND se.token_out_symbol != 'UNKNOWN'
       ${kolFilter}
     GROUP BY token_out_address
     ORDER BY ${orderBy}
     LIMIT ${limit}`,
    []
  );

  // Enriquecer com preços do DexScreener em paralelo (máx 5 simultâneos)
  const enriched: TokenStats[] = [];
  const BATCH = 5;
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const results = await Promise.all(
      batch.map(async (r) => {
        let price: number | null = null;
        let priceChange: number | null = null;
        let marketCap: number | null = null;
        let liquidity: number | null = null;
        let volume24h: number | null = null;

        try {
          const dex = await fetchDexScreenerData(r.token_address);
          if (dex) {
            price = dex.priceUsd ? parseFloat(dex.priceUsd) : null;
            priceChange = dex.priceChange?.h24 ?? null;
            marketCap = dex.marketCap ?? null;
            liquidity = dex.liquidity?.usd ?? null;
            volume24h = dex.volume?.h24 ?? null;
          }
        } catch {
          price = await getTokenPriceFromDexScreener(r.token_address);
        }

        return {
          address: r.token_address,
          symbol: r.token_symbol,
          name: null,
          price_usd: price,
          price_change_24h: priceChange,
          market_cap_usd: marketCap,
          liquidity_usd: liquidity,
          volume_24h_usd: volume24h,
          swap_count_24h: Number(r.swap_count),
          swap_count_7d: 0,
          unique_traders_24h: Number(r.unique_traders),
          unique_traders_7d: 0,
          total_volume_usd_24h: Number(r.total_volume_usd),
          total_volume_usd_7d: 0,
          buy_count_24h: Number(r.buy_count),
          sell_count_24h: Number(r.sell_count),
          avg_trade_size_usd: r.avg_trade_size_usd ? Number(r.avg_trade_size_usd) : null,
          top_kol_count: Number(r.kol_count),
        } as TokenStats;
      })
    );
    enriched.push(...results);
    if (i + BATCH < rows.length) {
      await new Promise((r) => setTimeout(r, 500)); // pausa entre batches
    }
  }

  return enriched;
}

/**
 * Retorna tokens que KOLs específicos estão comprando agora (últimas 2h)
 */
export async function getKolBuyingNow(limit = 20): Promise<{
  token_address: string;
  token_symbol: string | null;
  kol_count: number;
  total_volume_usd: number;
  price_usd: number | null;
  price_change_24h: number | null;
  kols: Array<{ address: string; label: string | null; follow_score: number | null }>;
}[]> {
  const rows = await query<{
    token_address: string;
    token_symbol: string | null;
    kol_count: number;
    total_volume_usd: number;
    kol_addresses: string;
    kol_labels: string;
    kol_scores: string;
  }>(
    `SELECT
       se.token_out_address                                           AS token_address,
       MAX(se.token_out_symbol)                                       AS token_symbol,
       COUNT(DISTINCT se.wallet_address)                              AS kol_count,
       COALESCE(SUM(ABS(se.value_usd)), 0)                           AS total_volume_usd,
       GROUP_CONCAT(DISTINCT se.wallet_address ORDER BY se.wallet_address) AS kol_addresses,
       GROUP_CONCAT(DISTINCT COALESCE(w.label, '') ORDER BY se.wallet_address) AS kol_labels,
       GROUP_CONCAT(DISTINCT COALESCE(
         (SELECT km.follow_score FROM kol_metrics km
          WHERE km.wallet_address = se.wallet_address AND km.period = 'weekly'
          ORDER BY km.period_start DESC LIMIT 1), 0)
         ORDER BY se.wallet_address
       ) AS kol_scores
     FROM swap_events se
     JOIN wallets w ON w.address = se.wallet_address
     WHERE se.timestamp >= NOW() - INTERVAL 2 HOUR
       AND w.is_disqualified = 0
       AND se.token_out_symbol IS NOT NULL
       AND se.token_out_symbol NOT IN ('WETH', 'USDC', 'USDT', 'ETH', 'UNKNOWN')
     GROUP BY se.token_out_address
     HAVING kol_count >= 2
     ORDER BY kol_count DESC, total_volume_usd DESC
     LIMIT ${limit}`,
    []
  );

  return Promise.all(
    rows.map(async (r) => {
      const addresses = (r.kol_addresses || '').split(',');
      const labels = (r.kol_labels || '').split(',');
      const scores = (r.kol_scores || '').split(',');

      const kols = addresses.map((addr, i) => ({
        address: addr,
        label: labels[i] || null,
        follow_score: scores[i] ? Number(scores[i]) : null,
      }));

      let price: number | null = null;
      let priceChange: number | null = null;
      try {
        const dex = await fetchDexScreenerData(r.token_address);
        if (dex) {
          price = dex.priceUsd ? parseFloat(dex.priceUsd) : null;
          priceChange = dex.priceChange?.h24 ?? null;
        }
      } catch { /* silent */ }

      return {
        token_address: r.token_address,
        token_symbol: r.token_symbol,
        kol_count: Number(r.kol_count),
        total_volume_usd: Number(r.total_volume_usd),
        price_usd: price,
        price_change_24h: priceChange,
        kols,
      };
    })
  );
}

/**
 * Retorna o histórico de preço on-chain de um token (baseado nos swaps)
 */
export async function getTokenPriceHistory(
  tokenAddress: string,
  period: 'daily' | 'weekly' | 'monthly' = 'weekly'
): Promise<Array<{
  hour: Date;
  avg_price_usd: number | null;
  volume_usd: number;
  swap_count: number;
}>> {
  const addr = tokenAddress.toLowerCase();

  let interval = '7 DAY';
  let groupBy = 'DATE_FORMAT(timestamp, \'%Y-%m-%d %H:00:00\')';
  if (period === 'daily') {
    interval = '24 HOUR';
    groupBy = 'DATE_FORMAT(timestamp, \'%Y-%m-%d %H:00:00\')';
  } else if (period === 'monthly') {
    interval = '30 DAY';
    groupBy = 'DATE_FORMAT(timestamp, \'%Y-%m-%d\')';
  }

  const rows = await query<{
    hour: Date;
    avg_price_usd: number | null;
    volume_usd: number;
    swap_count: number;
  }>(
    `SELECT
       ${groupBy}                          AS hour,
       AVG(ABS(value_usd) / NULLIF(token_out_amount / POW(10, 18), 0)) AS avg_price_usd,
       COALESCE(SUM(ABS(value_usd)), 0)   AS volume_usd,
       COUNT(*)                           AS swap_count
     FROM swap_events
     WHERE (token_in_address = ? OR token_out_address = ?)
       AND timestamp >= NOW() - INTERVAL ${interval}
       AND value_usd IS NOT NULL
     GROUP BY ${groupBy}
     ORDER BY hour ASC`,
    [addr, addr]
  );

  return rows;
}

/**
 * Verifica se um token tem swaps registrados
 */
async function tokenExistsInSwaps(tokenAddress: string): Promise<boolean> {
  const r = await query<{ cnt: number }>(
    `SELECT COUNT(*) AS cnt FROM swap_events
     WHERE token_in_address = ? OR token_out_address = ? LIMIT 1`,
    [tokenAddress, tokenAddress]
  );
  return Number(r[0]?.cnt) > 0;
}

/**
 * Atualiza market data de todos os tokens ativos no banco
 */
export async function updateTokenMarketData(): Promise<void> {
  logger.info('Updating token market data...');

  try {
    // Pegar tokens com mais swaps nas últimas 24h
    const activeTokens = await query<{ address: string }>(
      `SELECT DISTINCT token_out_address AS address
       FROM swap_events
       WHERE timestamp >= NOW() - INTERVAL 24 HOUR
         AND token_out_address IS NOT NULL
       GROUP BY token_out_address
       ORDER BY COUNT(*) DESC
       LIMIT 100`
    );

    let updated = 0;
    const BATCH = 5;

    for (let i = 0; i < activeTokens.length; i += BATCH) {
      const batch = activeTokens.slice(i, i + BATCH);
      await Promise.all(
        batch.map(async (t) => {
          const dex = await fetchDexScreenerData(t.address);
          if (!dex) return;

          const price = dex.priceUsd ? parseFloat(dex.priceUsd) : null;
          const symbol = dex.baseToken?.symbol || null;
          const name = dex.baseToken?.name || null;

          if (price !== null) {
            try {
              await execute(
                `INSERT INTO tokens (address, symbol, name, price_usd, price_updated_at)
                 VALUES (?, ?, ?, ?, NOW())
                 ON DUPLICATE KEY UPDATE
                   symbol = COALESCE(VALUES(symbol), symbol),
                   name   = COALESCE(VALUES(name), name),
                   price_usd = VALUES(price_usd),
                   price_updated_at = NOW()`,
                [t.address, symbol, name, price]
              );
              updated++;
            } catch { /* silent */ }
          }
        })
      );
      if (i + BATCH < activeTokens.length) {
        await new Promise((r) => setTimeout(r, 500));
      }
    }

    logger.info(`Token market data updated for ${updated} tokens`);
  } catch (error) {
    logger.error('Error updating token market data', { error: (error as Error).message });
  }
}
