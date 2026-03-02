import { ethers } from 'ethers';
import { config } from '../config';
import { logger } from '../utils/logger';
import { SwapEvent } from '../types';

// ABI do evento Swap do Uniswap V3
const UNISWAP_V3_SWAP_ABI = [
  'event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)',
];

// ABI do evento Swap do Uniswap V2 / Aerodrome V1
const UNISWAP_V2_SWAP_ABI = [
  'event Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)',
];

// ABI do evento Swap do Aerodrome V2
const AERODROME_V2_SWAP_ABI = [
  'event Swap(address indexed sender, address indexed to, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out)',
];

// ABI básica do par (para obter token0 e token1)
const PAIR_ABI = [
  'function token0() view returns (address)',
  'function token1() view returns (address)',
  'function getReserves() view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)',
];

// ABI básica do token ERC20
const ERC20_ABI = [
  'function symbol() view returns (string)',
  'function name() view returns (string)',
  'function decimals() view returns (uint8)',
];

// Interfaces para decodificação
const uniV3Interface = new ethers.Interface(UNISWAP_V3_SWAP_ABI);
const uniV2Interface = new ethers.Interface(UNISWAP_V2_SWAP_ABI);
const aeroV2Interface = new ethers.Interface(AERODROME_V2_SWAP_ABI);

// Topics dos eventos de Swap
export const SWAP_TOPICS = {
  UNISWAP_V3: uniV3Interface.getEvent('Swap')!.topicHash,
  UNISWAP_V2: uniV2Interface.getEvent('Swap')!.topicHash,
  AERODROME_V2: aeroV2Interface.getEvent('Swap')!.topicHash,
};

// Mapeamento de DEX por endereço
const DEX_NAMES: Record<string, string> = {
  [config.dexAddresses.uniswapV3Router.toLowerCase()]: 'Uniswap V3',
  [config.dexAddresses.aerodromeRouter.toLowerCase()]: 'Aerodrome',
  [config.dexAddresses.baseswapRouter.toLowerCase()]: 'BaseSwap',
  [config.dexAddresses.sushiswapRouter.toLowerCase()]: 'SushiSwap',
  [config.dexAddresses.swapbasedRouter.toLowerCase()]: 'SwapBased',
};

/**
 * Identifica o nome da DEX pelo endereço
 */
export function getDexName(address: string): string {
  return DEX_NAMES[address.toLowerCase()] || 'Unknown DEX';
}

/**
 * Verifica se um log é um evento de Swap
 */
export function isSwapEvent(log: ethers.Log): boolean {
  const topic = log.topics[0];
  return (
    topic === SWAP_TOPICS.UNISWAP_V3 ||
    topic === SWAP_TOPICS.UNISWAP_V2 ||
    topic === SWAP_TOPICS.AERODROME_V2
  );
}

/**
 * Decodifica um evento de Swap do Uniswap V3
 */
export function decodeUniswapV3Swap(
  log: ethers.Log
): {
  sender: string;
  recipient: string;
  amount0: bigint;
  amount1: bigint;
} | null {
  try {
    const decoded = uniV3Interface.parseLog({
      topics: log.topics as string[],
      data: log.data,
    });

    if (!decoded) return null;

    return {
      sender: decoded.args.sender as string,
      recipient: decoded.args.recipient as string,
      amount0: decoded.args.amount0 as bigint,
      amount1: decoded.args.amount1 as bigint,
    };
  } catch (error) {
    logger.debug('Failed to decode Uniswap V3 swap', { error: (error as Error).message });
    return null;
  }
}

/**
 * Decodifica um evento de Swap do Uniswap V2
 */
export function decodeUniswapV2Swap(
  log: ethers.Log
): {
  sender: string;
  to: string;
  amount0In: bigint;
  amount1In: bigint;
  amount0Out: bigint;
  amount1Out: bigint;
} | null {
  try {
    const decoded = uniV2Interface.parseLog({
      topics: log.topics as string[],
      data: log.data,
    });

    if (!decoded) return null;

    return {
      sender: decoded.args.sender as string,
      to: decoded.args.to as string,
      amount0In: decoded.args.amount0In as bigint,
      amount1In: decoded.args.amount1In as bigint,
      amount0Out: decoded.args.amount0Out as bigint,
      amount1Out: decoded.args.amount1Out as bigint,
    };
  } catch (error) {
    logger.debug('Failed to decode Uniswap V2 swap', { error: (error as Error).message });
    return null;
  }
}

/**
 * Processa logs de uma transação para extrair eventos de swap
 */
export async function extractSwapEvents(
  txHash: string,
  blockNumber: number,
  timestamp: Date,
  logs: ethers.Log[],
  provider: ethers.JsonRpcProvider
): Promise<SwapEvent[]> {
  const swapEvents: SwapEvent[] = [];

  for (const log of logs) {
    if (!isSwapEvent(log)) continue;

    try {
      const topic = log.topics[0];
      let swapData: SwapEvent | null = null;

      if (topic === SWAP_TOPICS.UNISWAP_V3) {
        swapData = await processUniswapV3Log(log, txHash, blockNumber, timestamp, provider);
      } else if (topic === SWAP_TOPICS.UNISWAP_V2 || topic === SWAP_TOPICS.AERODROME_V2) {
        swapData = await processUniswapV2Log(log, txHash, blockNumber, timestamp, provider);
      }

      if (swapData) {
        swapEvents.push(swapData);
      }
    } catch (error) {
      logger.debug('Error processing swap log', {
        txHash,
        logAddress: log.address,
        error: (error as Error).message,
      });
    }
  }

  return swapEvents;
}

/**
 * Processa um log de Swap do Uniswap V3
 */
async function processUniswapV3Log(
  log: ethers.Log,
  txHash: string,
  blockNumber: number,
  timestamp: Date,
  provider: ethers.JsonRpcProvider
): Promise<SwapEvent | null> {
  const decoded = decodeUniswapV3Swap(log);
  if (!decoded) return null;

  const pairContract = new ethers.Contract(log.address, PAIR_ABI, provider);

  try {
    const [token0Address, token1Address] = await Promise.all([
      pairContract.token0(),
      pairContract.token1(),
    ]);

    // Determinar qual token entrou e qual saiu baseado nos amounts
    const amount0IsNegative = decoded.amount0 < 0n;
    const tokenIn = amount0IsNegative ? token1Address : token0Address;
    const tokenOut = amount0IsNegative ? token0Address : token1Address;
    const amountIn = amount0IsNegative ? decoded.amount1 : decoded.amount0;
    const amountOut = amount0IsNegative ? -decoded.amount0 : -decoded.amount1;

    return {
      tx_hash: txHash,
      block_number: blockNumber,
      timestamp,
      wallet_address: decoded.recipient.toLowerCase(),
      dex_address: log.address.toLowerCase(),
      dex_name: getDexName(log.address),
      token_in_address: tokenIn.toLowerCase(),
      token_in_symbol: 'UNKNOWN',
      token_in_amount: amountIn.toString(),
      token_out_address: tokenOut.toLowerCase(),
      token_out_symbol: 'UNKNOWN',
      token_out_amount: amountOut.toString(),
    };
  } catch (error) {
    logger.debug('Error getting pair tokens for V3 swap', { error: (error as Error).message });
    return null;
  }
}

/**
 * Processa um log de Swap do Uniswap V2 / Aerodrome
 */
async function processUniswapV2Log(
  log: ethers.Log,
  txHash: string,
  blockNumber: number,
  timestamp: Date,
  provider: ethers.JsonRpcProvider
): Promise<SwapEvent | null> {
  const decoded = decodeUniswapV2Swap(log);
  if (!decoded) return null;

  const pairContract = new ethers.Contract(log.address, PAIR_ABI, provider);

  try {
    const [token0Address, token1Address] = await Promise.all([
      pairContract.token0(),
      pairContract.token1(),
    ]);

    // Determinar qual token entrou e qual saiu
    let tokenIn: string, tokenOut: string, amountIn: bigint, amountOut: bigint;

    if (decoded.amount0In > 0n) {
      tokenIn = token0Address;
      tokenOut = token1Address;
      amountIn = decoded.amount0In;
      amountOut = decoded.amount1Out;
    } else {
      tokenIn = token1Address;
      tokenOut = token0Address;
      amountIn = decoded.amount1In;
      amountOut = decoded.amount0Out;
    }

    return {
      tx_hash: txHash,
      block_number: blockNumber,
      timestamp,
      wallet_address: decoded.to.toLowerCase(),
      dex_address: log.address.toLowerCase(),
      dex_name: getDexName(log.address),
      token_in_address: tokenIn.toLowerCase(),
      token_in_symbol: 'UNKNOWN',
      token_in_amount: amountIn.toString(),
      token_out_address: tokenOut.toLowerCase(),
      token_out_symbol: 'UNKNOWN',
      token_out_amount: amountOut.toString(),
    };
  } catch (error) {
    logger.debug('Error getting pair tokens for V2 swap', { error: (error as Error).message });
    return null;
  }
}
