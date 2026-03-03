import { ethers } from 'ethers';
import { config } from '../config';
import { logger } from '../utils/logger';
import { retry } from '../utils/helpers';

// ─────────────────────────────────────────────────────────────────────────────
// O ethers.js v6 emite um aviso "JsonRpcProvider failed to detect network"
// quando tenta detectar a rede automaticamente na inicialização.
// Para evitar isso, passamos staticNetwork com chainId e name explícitos,
// o que desabilita a detecção automática.
// O provider de fallback é criado de forma LAZY (somente quando necessário),
// evitando conexões desnecessárias e ruído nos logs.
// ─────────────────────────────────────────────────────────────────────────────

const NETWORK: ethers.Networkish = {
  chainId: config.blockchain.chainId,
  name: config.blockchain.chainName,
};

let provider: ethers.JsonRpcProvider | null = null;
let fallbackProvider: ethers.JsonRpcProvider | null = null;

/**
 * Cria um JsonRpcProvider com rede estática (sem detecção automática).
 * staticNetwork evita o aviso "failed to detect network" do ethers v6.
 */
function createProvider(rpcUrl: string): ethers.JsonRpcProvider {
  return new ethers.JsonRpcProvider(rpcUrl, NETWORK, {
    staticNetwork: ethers.Network.from(NETWORK),
  });
}

/**
 * Retorna o provider principal (singleton, lazy)
 */
export function getProvider(): ethers.JsonRpcProvider {
  if (!provider) {
    provider = createProvider(config.blockchain.rpcUrl);
    logger.info('Blockchain provider initialized', {
      rpcUrl: config.blockchain.rpcUrl,
      chainId: config.blockchain.chainId,
    });
  }
  return provider;
}

/**
 * Retorna o provider de fallback (singleton, lazy — criado apenas quando necessário)
 */
function getFallbackProvider(): ethers.JsonRpcProvider {
  if (!fallbackProvider) {
    fallbackProvider = createProvider(config.blockchain.rpcUrlFallback);
    logger.warn('Switching to fallback RPC provider', {
      rpcUrl: config.blockchain.rpcUrlFallback,
    });
  }
  return fallbackProvider;
}

/**
 * Executa uma chamada RPC com fallback automático em caso de falha
 */
async function withFallback<T>(
  fn: (p: ethers.JsonRpcProvider) => Promise<T>
): Promise<T> {
  return retry(async () => {
    try {
      return await fn(getProvider());
    } catch (error) {
      logger.warn('Primary RPC failed, retrying with fallback', {
        error: (error as Error).message,
      });
      return await fn(getFallbackProvider());
    }
  }, config.blockchain.maxRetries);
}

/**
 * Obtém o número do bloco mais recente
 */
export async function getLatestBlockNumber(): Promise<number> {
  return withFallback((p) => p.getBlockNumber());
}

/**
 * Obtém um bloco pelo número
 */
export async function getBlock(blockNumber: number): Promise<ethers.Block | null> {
  return withFallback((p) => p.getBlock(blockNumber));
}

/**
 * Obtém um bloco com todas as transações prefetchadas
 */
export async function getBlockWithTransactions(
  blockNumber: number
): Promise<ethers.Block | null> {
  return withFallback((p) => p.getBlock(blockNumber, true));
}

/**
 * Obtém o recibo de uma transação
 */
export async function getTransactionReceipt(
  txHash: string
): Promise<ethers.TransactionReceipt | null> {
  return withFallback((p) => p.getTransactionReceipt(txHash));
}

/**
 * Obtém logs de eventos filtrados
 */
export async function getLogs(filter: ethers.Filter): Promise<ethers.Log[]> {
  return withFallback((p) => p.getLogs(filter));
}

/**
 * Testa a conexão com o provider principal
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
  const balance = await withFallback((p) => p.getBalance(address));
  return parseFloat(ethers.formatEther(balance));
}

/**
 * Verifica se um endereço é um contrato
 */
export async function isContract(address: string): Promise<boolean> {
  const code = await withFallback((p) => p.getCode(address));
  return code !== '0x';
}

/**
 * Destrói os providers (para shutdown limpo)
 */
export function destroyProviders(): void {
  provider?.destroy();
  fallbackProvider?.destroy();
  provider = null;
  fallbackProvider = null;
}
