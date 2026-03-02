import axios from 'axios';
import { logger } from '../utils/logger';
import { query } from '../database/connection';
import { retry } from '../utils/helpers';

const DEXSCREENER_BASE_URL = 'https://api.dexscreener.com/latest/dex';
const COINGECKO_BASE_URL = 'https://api.coingecko.com/api/v3';

// Cache em memória para preços de tokens
const priceCache = new Map<string, { price: number; timestamp: number }>();
const CACHE_TTL_MS = 30 * 1000; // 30 segundos

// Endereços de tokens nativos/wrapped na Base
const WETH_ADDRESS = '0x4200000000000000000000000000000000000006';
const USDC_ADDRESS = '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913';
const USDT_ADDRESS = '0xfde4c96c8593536e31f229ea8f37b2ada2699bb2';

/**
 * Obtém o preço de um token em USD via DexScreener
 */
export async function getTokenPriceFromDexScreener(
  tokenAddress: string
): Promise<number | null> {
  try {
    const url = `${DEXSCREENER_BASE_URL}/tokens/${tokenAddress}`;
    const response = await axios.get(url, { timeout: 5000 });

    if (!response.data?.pairs?.length) return null;

    // Filtrar pares na Base e ordenar por liquidez
    const basePairs = response.data.pairs
      .filter((pair: { chainId: string; liquidity?: { usd?: number }; priceUsd?: string }) => pair.chainId === 'base')
      .sort((a: { liquidity?: { usd?: number } }, b: { liquidity?: { usd?: number } }) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0));

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
 * Obtém o preço do ETH em USD via CoinGecko
 */
export async function getEthPriceUsd(): Promise<number> {
  const cacheKey = 'eth_price';
  const cached = priceCache.get(cacheKey);

  if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
    return cached.price;
  }

  try {
    const response = await retry(async () => {
      return axios.get(
        `${COINGECKO_BASE_URL}/simple/price?ids=ethereum&vs_currencies=usd`,
        { timeout: 5000 }
      );
    }, 3);

    const price = response.data?.ethereum?.usd;
    if (price) {
      priceCache.set(cacheKey, { price, timestamp: Date.now() });
      return price;
    }
  } catch (error) {
    logger.warn('CoinGecko ETH price fetch failed', { error: (error as Error).message });
  }

  // Fallback para DexScreener
  try {
    const price = await getTokenPriceFromDexScreener(WETH_ADDRESS);
    if (price) {
      priceCache.set(cacheKey, { price, timestamp: Date.now() });
      return price;
    }
  } catch (error) {
    logger.warn('DexScreener ETH price fetch failed', { error: (error as Error).message });
  }

  // Retornar último preço em cache ou valor padrão
  return cached?.price || 3000;
}

/**
 * Obtém o preço de um token com cache
 */
export async function getTokenPrice(tokenAddress: string): Promise<number | null> {
  const normalizedAddress = tokenAddress.toLowerCase();

  // Verificar cache em memória
  const cached = priceCache.get(normalizedAddress);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
    return cached.price;
  }

  // Verificar banco de dados
  try {
    const dbResult = await query<{ price_usd: number; price_updated_at: Date }>(
      `SELECT price_usd, price_updated_at FROM tokens 
       WHERE address = $1 AND price_updated_at > NOW() - INTERVAL '5 minutes'`,
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

  // Buscar preço da API
  let price: number | null = null;

  // Tokens estáveis
  if (normalizedAddress === USDC_ADDRESS || normalizedAddress === USDT_ADDRESS) {
    price = 1.0;
  } else if (normalizedAddress === WETH_ADDRESS) {
    price = await getEthPriceUsd();
  } else {
    price = await getTokenPriceFromDexScreener(normalizedAddress);
  }

  if (price !== null) {
    priceCache.set(normalizedAddress, { price, timestamp: Date.now() });

    // Salvar no banco de dados
    try {
      await query(
        `INSERT INTO tokens (address, price_usd, price_updated_at) 
         VALUES ($1, $2, NOW())
         ON CONFLICT (address) DO UPDATE SET price_usd = $2, price_updated_at = NOW(), updated_at = NOW()`,
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

  // Verificar banco de dados
  try {
    const dbResult = await query<{ symbol: string; name: string; decimals: number }>(
      'SELECT symbol, name, decimals FROM tokens WHERE address = $1',
      [normalizedAddress]
    );

    if (dbResult.length > 0 && dbResult[0].symbol) {
      return dbResult[0];
    }
  } catch (error) {
    logger.debug('DB token info fetch failed', { token: normalizedAddress });
  }

  // Buscar via DexScreener
  try {
    const url = `${DEXSCREENER_BASE_URL}/tokens/${normalizedAddress}`;
    const response = await axios.get(url, { timeout: 5000 });

    if (response.data?.pairs?.length) {
      const basePairs = response.data.pairs.filter(
        (p: { chainId: string }) => p.chainId === 'base'
      );

      if (basePairs.length > 0) {
        const pair = basePairs[0];
        const isToken0 =
          pair.baseToken.address.toLowerCase() === normalizedAddress;
        const tokenData = isToken0 ? pair.baseToken : pair.quoteToken;

        const info = {
          symbol: tokenData.symbol || 'UNKNOWN',
          name: tokenData.name || 'Unknown Token',
          decimals: 18, // padrão ERC20
        };

        // Salvar no banco de dados
        try {
          await query(
            `INSERT INTO tokens (address, symbol, name, decimals) 
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (address) DO UPDATE SET symbol = $2, name = $3, updated_at = NOW()`,
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
 * Atualiza preços de todos os tokens no banco de dados
 */
export async function updateAllTokenPrices(): Promise<void> {
  logger.info('Updating all token prices...');

  try {
    const tokens = await query<{ address: string }>(
      'SELECT address FROM tokens WHERE price_updated_at < NOW() - INTERVAL \'5 minutes\' OR price_updated_at IS NULL LIMIT 100'
    );

    for (const token of tokens) {
      await getTokenPrice(token.address);
    }

    logger.info(`Updated prices for ${tokens.length} tokens`);
  } catch (error) {
    logger.error('Error updating token prices', { error: (error as Error).message });
  }
}
