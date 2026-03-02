import { ethers } from 'ethers';

/**
 * Formata um endereço Ethereum para lowercase
 */
export function normalizeAddress(address: string): string {
  return address.toLowerCase();
}

/**
 * Verifica se um endereço é válido
 */
export function isValidAddress(address: string): boolean {
  return ethers.isAddress(address);
}

/**
 * Converte wei para ETH
 */
export function weiToEth(wei: bigint | string): number {
  return parseFloat(ethers.formatEther(wei));
}

/**
 * Converte ETH para wei
 */
export function ethToWei(eth: number): bigint {
  return ethers.parseEther(eth.toString());
}

/**
 * Formata um valor em ETH para exibição
 */
export function formatEth(value: number, decimals: number = 4): string {
  return value.toFixed(decimals);
}

/**
 * Formata um valor em USD para exibição
 */
export function formatUsd(value: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

/**
 * Calcula a data de início de um período
 */
export function getPeriodStart(period: 'daily' | 'weekly' | 'monthly' | 'all_time'): Date {
  const now = new Date();

  switch (period) {
    case 'daily':
      return new Date(now.getTime() - 24 * 60 * 60 * 1000);
    case 'weekly':
      return new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    case 'monthly':
      return new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    case 'all_time':
      return new Date(0);
    default:
      return new Date(now.getTime() - 24 * 60 * 60 * 1000);
  }
}

/**
 * Trunca um endereço para exibição
 */
export function truncateAddress(address: string, chars: number = 6): string {
  if (address.length <= chars * 2 + 2) return address;
  return `${address.slice(0, chars + 2)}...${address.slice(-chars)}`;
}

/**
 * Formata um timestamp Unix para Date
 */
export function unixToDate(timestamp: number): Date {
  return new Date(timestamp * 1000);
}

/**
 * Calcula a taxa de vitória
 */
export function calculateWinRate(wins: number, total: number): number {
  if (total === 0) return 0;
  return (wins / total) * 100;
}

/**
 * Aguarda um número de milissegundos
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Retenta uma função assíncrona com backoff exponencial
 */
export async function retry<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  baseDelay: number = 1000
): Promise<T> {
  let lastError: Error | undefined;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as Error;
      if (attempt < maxRetries - 1) {
        const delay = baseDelay * Math.pow(2, attempt);
        await sleep(delay);
      }
    }
  }

  throw lastError;
}

/**
 * Divide um array em chunks
 */
export function chunkArray<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

/**
 * Formata um número grande com sufixos (K, M, B)
 */
export function formatLargeNumber(num: number): string {
  if (num >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(2)}B`;
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(2)}M`;
  if (num >= 1_000) return `${(num / 1_000).toFixed(2)}K`;
  return num.toFixed(2);
}
