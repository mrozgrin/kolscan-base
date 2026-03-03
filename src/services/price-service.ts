import axios from 'axios';
import { logger } from '../utils/logger';
import { query, execute } from '../database/connection';

const DEXSCREENER_BASE_URL = 'https://api.dexscreener.com/latest/dex';
const COINGECKO_BASE_URL = 'https://api.coingecko.com/api/v3';

// ─────────────────────────────────────────────────────────────────────────────
// Cache em memória
// ─────────────────────────────────────────────────────────────────────────────
const priceCache = new Map<string, { price: number; timestamp: number }>();

// TTLs diferenciados por fonte
const CACHE_TTL_MEMORY_MS  = 5 * 60 * 1000;  // 5 min — cache em memória
const CACHE_TTL_DB_MS      = 10 * 60 * 1000; // 10 min — cache no banco

// Endereços de tokens nativos/wrapped na Base
const WETH_ADDRESS = '0x4200000000000000000000000000000000000006';
const USDC_ADDRESS = '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913';
const USDT_ADDRESS = '0xfde4c96c8593536e31f229ea8f37b2ada2699bb2';

// ─────────────────────────────────────────────────────────────────────────────
// Rate limiter simples para CoinGecko (plano gratuito: ~30 req/min)
// Garantimos no máximo 1 chamada a cada 3 segundos para a CoinGecko.
// ─────────────────────────────────────────────────────────────────────────────
let lastCoinGeckoCallAt = 0;
const COINGECKO_MIN_INTERVAL_MS = 3000; // 3s entre chamadas = ~20 req/min

async function coinGeckoThrottle(): Promise<void> {
  const now = Date.now();
  const elapsed = now - lastCoinGeckoCallAt;
  if (elapsed < COINGECKO_MIN_INTERVAL_MS) {
    await new Promise((resolve) =>
      setTimeout(resolve, COINGECKO_MIN_INTERVAL_MS - elapsed)
    );
  }
  lastCoinGeckoCallAt = Date.now();
}

/**
 * Obtém o preço do ETH em USD via CoinGecko (com throttle e fallback para DexScreener)
 */
export async function getEthPriceUsd(): Promise<number> {
  const cacheKey = 'eth_price';
  const cached = priceCache.get(cacheKey);

  if (cached && Date.now() - cached.timestamp < CACHE_TTL_MEMORY_MS) {
    return cached.price;
  }

  // Tentar CoinGecko com throttle
  try {
    await coinGeckoThrottle();
    const response = await axios.get(
      `${COINGECKO_BASE_URL}/simple/price?ids=ethereum&vs_currencies=usd`,
      { timeout: 8000 }
    );

    const price = response.data?.ethereum?.usd;
    if (price) {
      priceCache.set(cacheKey, { price, timestamp: Date.now() });
      return price;
    }
  } catch (error) {
    const status = (error as { response?: { status?: number } }).response?.status;
    if (status === 429) {
      logger.debug('CoinGecko rate limited, falling back to DexScreener for ETH price');
    } else {
      logger.warn('CoinGecko ETH price fetch failed', { error: (error as Error).message });
    }
  }

  // Fallback para DexScreener (sem rate limit)
  try {
    const price = await getTokenPriceFromDexScreener(WETH_ADDRESS);
    if (price) {
      priceCache.set(cacheKey, { price, timestamp: Date.now() });
      return price;
    }
  } catch (error) {
    logger.debug('DexScreener ETH price fetch also failed', { error: (error as Error).message });
  }

  // Retornar último preço em cache ou valor padrão conservador
  return cached?.price || 3000;
}

/**
 * Obtém o preço de um token em USD via DexScreener
 */
export async function getTokenPriceFromDexScreener(
  tokenAddress: string
): Promise<number | null> {
  try {
    const url = `${DEXSCREENER_BASE_URL}/tokens/${tokenAddress}`;
    const response = await axios.get(url, { timeout: 8000 });

    if (!response.data?.pairs?.length) return null;

    const basePairs = response.data.pairs
      .filter((pair: { chainId: string; liquidity?: { usd?: number }; priceUsd?: string }) =>
        pair.chainId === 'base'
      )
      .sort(
        (a: { liquidity?: { usd?: number } }, b: { liquidity?: { usd?: number } }) =>
          (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0)
      );

    if (!basePairs.length) return null;

    const price = parseFloat(basePairs[0].priceUsd);
    return isNaN(price) ? null : price;
  } catch (error) {
    logger.debug('DexScreener price fetch failed', {
      token: tokenAddress,
      error: (error as Error).message,
    });
    return null;
  }
}

/**
 * Obtém o preço de um token com cache em memória e banco de dados
 */
export async function getTokenPrice(tokenAddress: string): Promise<number | null> {
  const normalizedAddress = tokenAddress.toLowerCase();

  // 1. Cache em memória (mais rápido)
  const cached = priceCache.get(normalizedAddress);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL_MEMORY_MS) {
    return cached.price;
  }

  // 2. Cache no banco de dados
  try {
    const dbResult = await query<{ price_usd: number; price_updated_at: Date }>(
      `SELECT price_usd, price_updated_at FROM tokens
       WHERE address = ? AND price_updated_at > NOW() - INTERVAL 10 MINUTE`,
      [normalizedAddress]
    );

    if (dbResult.length > 0 && dbResult[0].price_usd) {
      const price = parseFloat(dbResult[0].price_usd.toString());
      priceCache.set(normalizedAddress, { price, timestamp: Date.now() });
      return price;
    }
  } catch (error) {
    logger.debug('DB price fetch failed', { token: normalizedAddress });
  }

  // 3. Buscar da API externa
  let price: number | null = null;

  if (normalizedAddress === USDC_ADDRESS || normalizedAddress === USDT_ADDRESS) {
    price = 1.0;
  } else if (normalizedAddress === WETH_ADDRESS) {
    price = await getEthPriceUsd();
  } else {
    price = await getTokenPriceFromDexScreener(normalizedAddress);
  }

  if (price !== null) {
    priceCache.set(normalizedAddress, { price, timestamp: Date.now() });

    try {
      await execute(
        `INSERT INTO tokens (address, price_usd, price_updated_at)
         VALUES (?, ?, NOW())
         ON DUPLICATE KEY UPDATE price_usd = VALUES(price_usd), price_updated_at = NOW()`,
        [normalizedAddress, price]
      );
    } catch (error) {
      logger.debug('Failed to save token price to DB', { error: (error as Error).message });
    }
  }

  return price;
}

/**
 * Obtém informações de um token (símbolo, nome, decimais)
 */
export async function getTokenInfo(
  tokenAddress: string
): Promise<{ symbol: string; name: string; decimals: number } | null> {
  const normalizedAddress = tokenAddress.toLowerCase();

  try {
    const dbResult = await query<{ symbol: string; name: string; decimals: number }>(
      'SELECT symbol, name, decimals FROM tokens WHERE address = ?',
      [normalizedAddress]
    );

    if (dbResult.length > 0 && dbResult[0].symbol) {
      return dbResult[0];
    }
  } catch (error) {
    logger.debug('DB token info fetch failed', { token: normalizedAddress });
  }

  try {
    const url = `${DEXSCREENER_BASE_URL}/tokens/${normalizedAddress}`;
    const response = await axios.get(url, { timeout: 8000 });

    if (response.data?.pairs?.length) {
      const basePairs = response.data.pairs.filter(
        (p: { chainId: string }) => p.chainId === 'base'
      );

      if (basePairs.length > 0) {
        const pair = basePairs[0];
        const isToken0 = pair.baseToken.address.toLowerCase() === normalizedAddress;
        const tokenData = isToken0 ? pair.baseToken : pair.quoteToken;

        const info = {
          symbol: tokenData.symbol || 'UNKNOWN',
          name: tokenData.name || 'Unknown Token',
          decimals: 18,
        };

        try {
          await execute(
            `INSERT INTO tokens (address, symbol, name, decimals)
             VALUES (?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE symbol = VALUES(symbol), name = VALUES(name)`,
            [normalizedAddress, info.symbol, info.name, info.decimals]
          );
        } catch (dbError) {
          logger.debug('Failed to save token info to DB');
        }

        return info;
      }
    }
  } catch (error) {
    logger.debug('DexScreener token info fetch failed', { error: (error as Error).message });
  }

  return null;
}

/**
 * Atualiza preços de todos os tokens no banco de dados.
 * Processa em lotes de 10 para não sobrecarregar as APIs externas.
 */
export async function updateAllTokenPrices(): Promise<void> {
  logger.info('Updating all token prices...');

  try {
    const tokens = await query<{ address: string }>(
      `SELECT address FROM tokens
       WHERE price_updated_at < NOW() - INTERVAL ${CACHE_TTL_DB_MS / 60000} MINUTE
          OR price_updated_at IS NULL
       LIMIT 50`
    );

    // Processar em lotes de 10 com pausa entre lotes para respeitar rate limits
    const BATCH_SIZE = 10;
    let updated = 0;

    for (let i = 0; i < tokens.length; i += BATCH_SIZE) {
      const batch = tokens.slice(i, i + BATCH_SIZE);
      await Promise.all(batch.map((t) => getTokenPrice(t.address)));
      updated += batch.length;

      // Pausa de 1s entre lotes para não saturar DexScreener
      if (i + BATCH_SIZE < tokens.length) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }

    logger.info(`Updated prices for ${updated} tokens`);
  } catch (error) {
    logger.error('Error updating token prices', { error: (error as Error).message });
  }
}
