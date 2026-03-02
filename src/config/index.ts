import dotenv from 'dotenv';

dotenv.config();

export const config = {
  // Server
  port: parseInt(process.env.PORT || '3000', 10),
  nodeEnv: process.env.NODE_ENV || 'development',

  // Database
  database: {
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432', 10),
    name: process.env.DB_NAME || 'kolscan_base',
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD || '',
    ssl: process.env.DB_SSL === 'true',
    poolMax: parseInt(process.env.DB_POOL_MAX || '20', 10),
    poolMin: parseInt(process.env.DB_POOL_MIN || '2', 10),
    poolIdleTimeout: parseInt(process.env.DB_POOL_IDLE_TIMEOUT || '30000', 10),
  },

  // Redis (para cache e filas de jobs)
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    password: process.env.REDIS_PASSWORD || undefined,
    db: parseInt(process.env.REDIS_DB || '0', 10),
  },

  // Blockchain - Base
  blockchain: {
    // RPC URL principal (Alchemy, QuickNode, etc.)
    rpcUrl: process.env.BASE_RPC_URL || 'https://mainnet.base.org',
    // RPC URL de fallback
    rpcUrlFallback: process.env.BASE_RPC_URL_FALLBACK || 'https://base.llamarpc.com',
    // Chain ID da Base Mainnet
    chainId: 8453,
    chainName: 'Base',
    // Bloco inicial para indexação (pode ser ajustado para um bloco mais recente)
    startBlock: parseInt(process.env.START_BLOCK || '0', 10),
    // Número de blocos a processar por batch
    batchSize: parseInt(process.env.BATCH_SIZE || '100', 10),
    // Intervalo de polling em ms
    pollingInterval: parseInt(process.env.POLLING_INTERVAL || '2000', 10),
    // Número máximo de retentativas em caso de erro
    maxRetries: parseInt(process.env.MAX_RETRIES || '3', 10),
  },

  // API Keys
  apiKeys: {
    alchemy: process.env.ALCHEMY_API_KEY || '',
    coinbaseDevPlatform: process.env.CDP_API_KEY || '',
    // Chave para a API de preços de tokens
    coingecko: process.env.COINGECKO_API_KEY || '',
    // Chave para a API do DexScreener
    dexscreener: process.env.DEXSCREENER_API_KEY || '',
  },

  // Indexer
  indexer: {
    // Habilitar ou desabilitar o indexador
    enabled: process.env.INDEXER_ENABLED !== 'false',
    // Intervalo para atualizar métricas dos KOLs (em ms)
    metricsUpdateInterval: parseInt(process.env.METRICS_UPDATE_INTERVAL || '300000', 10), // 5 min
    // Número mínimo de trades para um endereço ser considerado KOL
    minTradesForKol: parseInt(process.env.MIN_TRADES_FOR_KOL || '5', 10),
    // Número de KOLs no leaderboard
    leaderboardSize: parseInt(process.env.LEADERBOARD_SIZE || '100', 10),
  },

  // Cache
  cache: {
    // TTL do cache do leaderboard em segundos
    leaderboardTtl: parseInt(process.env.LEADERBOARD_CACHE_TTL || '60', 10),
    // TTL do cache dos detalhes de um KOL em segundos
    kolDetailsTtl: parseInt(process.env.KOL_DETAILS_CACHE_TTL || '120', 10),
    // TTL do cache de preços de tokens em segundos
    tokenPriceTtl: parseInt(process.env.TOKEN_PRICE_CACHE_TTL || '30', 10),
  },

  // DEX Addresses na Base (para identificar swaps)
  dexAddresses: {
    // Uniswap V3 na Base
    uniswapV3Router: '0x2626664c2603336E57B271c5C0b26F421741e481',
    uniswapV3Factory: '0x33128a8fC17869897dcE68Ed026d694621f6FDfD',
    // Aerodrome (principal DEX da Base)
    aerodromeRouter: '0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43',
    aerodromeFactory: '0x420DD381b31aEf6683db6B902084cB0FFECe40Da',
    // BaseSwap
    baseswapRouter: '0x327Df1E6de05895d2ab08513aaDD9313Fe505d86',
    // SushiSwap na Base
    sushiswapRouter: '0x6BDED42c6DA8FBf0d2bA55B2fa120C5e0c8D7891',
    // SwapBased
    swapbasedRouter: '0xaaa3b1F1bd7BCc97fD1917c18ADE665C5D31F066',
  },

  // Logging
  logging: {
    level: process.env.LOG_LEVEL || 'info',
    format: process.env.LOG_FORMAT || 'json',
  },
};

export default config;
