import { ethers } from 'ethers';
import { config } from '../config';
import { logger } from '../utils/logger';
import { retry } from '../utils/helpers';

let provider: ethers.JsonRpcProvider | null = null;
let fallbackProvider: ethers.JsonRpcProvider | null = null;

/**
 * Obtém o provider principal da blockchain Base
 */
export function getProvider(): ethers.JsonRpcProvider {
  if (!provider) {
    provider = new ethers.JsonRpcProvider(config.blockchain.rpcUrl, {
      chainId: config.blockchain.chainId,
      name: config.blockchain.chainName,
    });

    logger.info('Blockchain provider initialized', {
      rpcUrl: config.blockchain.rpcUrl,
      chainId: config.blockchain.chainId,
    });
  }

  return provider;
}

/**
 * Obtém o provider de fallback
 */
export function getFallbackProvider(): ethers.JsonRpcProvider {
  if (!fallbackProvider) {
    fallbackProvider = new ethers.JsonRpcProvider(config.blockchain.rpcUrlFallback, {
      chainId: config.blockchain.chainId,
      name: config.blockchain.chainName,
    });
  }

  return fallbackProvider;
}

/**
 * Obtém o número do bloco mais recente
 */
export async function getLatestBlockNumber(): Promise<number> {
  return retry(async () => {
    try {
      return await getProvider().getBlockNumber();
    } catch (error) {
      logger.warn('Primary provider failed, trying fallback', { error: (error as Error).message });
      return await getFallbackProvider().getBlockNumber();
    }
  }, config.blockchain.maxRetries);
}

/**
 * Obtém um bloco pelo número
 */
export async function getBlock(
  blockNumber: number
): Promise<ethers.Block | null> {
  return retry(async () => {
    try {
      return await getProvider().getBlock(blockNumber);
    } catch (error) {
      return await getFallbackProvider().getBlock(blockNumber);
    }
  }, config.blockchain.maxRetries);
}

/**
 * Obtém um bloco com todas as transações
 */
export async function getBlockWithTransactions(
  blockNumber: number
): Promise<ethers.Block | null> {
  return retry(async () => {
    try {
      return await getProvider().getBlock(blockNumber, true);
    } catch (error) {
      return await getFallbackProvider().getBlock(blockNumber, true);
    }
  }, config.blockchain.maxRetries);
}

/**
 * Obtém o recibo de uma transação
 */
export async function getTransactionReceipt(
  txHash: string
): Promise<ethers.TransactionReceipt | null> {
  return retry(async () => {
    try {
      return await getProvider().getTransactionReceipt(txHash);
    } catch (error) {
      return await getFallbackProvider().getTransactionReceipt(txHash);
    }
  }, config.blockchain.maxRetries);
}

/**
 * Obtém logs de eventos filtrados
 */
export async function getLogs(
  filter: ethers.Filter
): Promise<ethers.Log[]> {
  return retry(async () => {
    try {
      return await getProvider().getLogs(filter);
    } catch (error) {
      return await getFallbackProvider().getLogs(filter);
    }
  }, config.blockchain.maxRetries);
}

/**
 * Testa a conexão com o provider
 */
export async function testProviderConnection(): Promise<boolean> {
  try {
    const blockNumber = await getLatestBlockNumber();
    logger.info('Blockchain provider connection successful', { blockNumber });
    return true;
  } catch (error) {
    logger.error('Blockchain provider connection failed', {
      error: (error as Error).message,
    });
    return false;
  }
}

/**
 * Obtém o saldo de uma carteira em ETH
 */
export async function getWalletBalance(address: string): Promise<number> {
  const balance = await getProvider().getBalance(address);
  return parseFloat(ethers.formatEther(balance));
}

/**
 * Verifica se um endereço é um contrato
 */
export async function isContract(address: string): Promise<boolean> {
  const code = await getProvider().getCode(address);
  return code !== '0x';
}
