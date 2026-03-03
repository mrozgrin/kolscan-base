import { ethers } from 'ethers';
import type { Block, TransactionResponse, Log } from 'ethers';
import { getProvider, getBlockWithTransactions, getLatestBlockNumber } from './provider';
import { extractSwapEvents, SWAP_TOPICS } from './dex-decoder';
import { query, execute } from '../database/connection';
// getTokenPrice and getTokenInfo are used indirectly via processSwapEvent
import { getTokenPrice, getTokenInfo } from '../services/price-service';
import { config } from '../config';
import { logger } from '../utils/logger';
import { sleep, chunkArray } from '../utils/helpers';
import { SwapEvent } from '../types';

interface IndexerStatus {
  isRunning: boolean;
  lastIndexedBlock: number;
  latestBlock: number;
  syncProgress: number;
  blocksPerSecond: number;
}

let isRunning = false;
let lastIndexedBlock = 0;

/**
 * Obtém o último bloco indexado do banco de dados
 */
async function getLastIndexedBlock(): Promise<number> {
  const result = await query<{ last_indexed_block: number }>(
    'SELECT last_indexed_block FROM indexer_state WHERE id = 1'
  );

  if (result.length > 0) {
    return result[0].last_indexed_block;
  }

  return config.blockchain.startBlock || 0;
}

/**
 * Atualiza o estado do indexador no banco de dados
 */
async function updateIndexerState(
  lastBlock: number,
  isSyncing: boolean,
  syncProgress?: number
): Promise<void> {
  await execute(
    `UPDATE indexer_state
     SET last_indexed_block = ?, is_syncing = ?, sync_progress = ?, last_updated = NOW()
     WHERE id = 1`,
    [lastBlock, isSyncing ? 1 : 0, syncProgress ?? null]
  );
}

/**
 * Processa uma transação e extrai eventos de swap
 */
async function processTransaction(
  tx: TransactionResponse,
  block: Block,
  provider: ethers.JsonRpcProvider
): Promise<void> {
  const timestamp = new Date(Number(block.timestamp) * 1000);
  const fromAddress = tx.from.toLowerCase();
  const toAddress = tx.to?.toLowerCase() || '';

  // Garantir que a carteira existe no banco de dados
  await execute(
    `INSERT INTO wallets (address, first_seen, last_seen, total_transactions)
     VALUES (?, ?, ?, 1)
     ON DUPLICATE KEY UPDATE
       last_seen = VALUES(last_seen),
       total_transactions = total_transactions + 1,
       updated_at = NOW()`,
    [fromAddress, timestamp, timestamp]
  );

  // Verificar se a transação tem valor em ETH
  const valueEth = parseFloat(ethers.formatEther(tx.value));

  // Obter recibo da transação para acessar os logs
  const receipt = await provider.getTransactionReceipt(tx.hash);
  if (!receipt) return;

  // Verificar se há eventos de swap nos logs
  const hasSwapEvents = receipt.logs.some(
    (log) =>
      log.topics[0] === SWAP_TOPICS.UNISWAP_V3 ||
      log.topics[0] === SWAP_TOPICS.UNISWAP_V2 ||
      log.topics[0] === SWAP_TOPICS.AERODROME_V2
  );

  if (!hasSwapEvents && valueEth === 0) return;

  // Extrair eventos de swap
  const swapEvents = await extractSwapEvents(
    tx.hash,
    Number(block.number),
    timestamp,
    [...receipt.logs] as Log[],
    provider
  );

  // Processar cada evento de swap
  for (const swapEvent of swapEvents) {
    await processSwapEvent(swapEvent, fromAddress);
  }

  // Salvar transação no banco de dados
  const txType = swapEvents.length > 0 ? 'swap' : valueEth > 0 ? 'transfer' : 'contract_call';

  await execute(
    `INSERT IGNORE INTO transactions
       (hash, wallet_address, block_number, timestamp, from_address, to_address, value_eth, tx_type)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      tx.hash,
      fromAddress,
      Number(block.number),
      timestamp,
      fromAddress,
      toAddress,
      valueEth,
      txType,
    ]
  );
}

/**
 * Processa um evento de swap e calcula PnL
 */
async function processSwapEvent(
  swapEvent: SwapEvent,
  walletAddress: string
): Promise<void> {
  // Obter informações dos tokens
  const [tokenInInfo, tokenOutInfo] = await Promise.all([
    getTokenInfo(swapEvent.token_in_address),
    getTokenInfo(swapEvent.token_out_address),
  ]);

  const tokenInSymbol = tokenInInfo?.symbol || 'UNKNOWN';
  const tokenOutSymbol = tokenOutInfo?.symbol || 'UNKNOWN';
  const tokenInDecimals = tokenInInfo?.decimals || 18;
  const tokenOutDecimals = tokenOutInfo?.decimals || 18;

  // Calcular valor em USD
  const [tokenInPrice, tokenOutPrice] = await Promise.all([
    getTokenPrice(swapEvent.token_in_address),
    getTokenPrice(swapEvent.token_out_address),
  ]);

  const tokenInAmount =
    parseFloat(swapEvent.token_in_amount) / Math.pow(10, tokenInDecimals);
  const tokenOutAmount =
    parseFloat(swapEvent.token_out_amount) / Math.pow(10, tokenOutDecimals);

  const valueInUsd = tokenInPrice ? tokenInAmount * tokenInPrice : null;
  const valueOutUsd = tokenOutPrice ? tokenOutAmount * tokenOutPrice : null;

  // Calcular PnL (simplificado: valor saída - valor entrada)
  let pnl: number | null = null;
  let isWin: boolean | null = null;

  if (valueInUsd !== null && valueOutUsd !== null) {
    pnl = valueOutUsd - valueInUsd;
    isWin = pnl > 0;
  }

  // Salvar evento de swap
  await execute(
    `INSERT IGNORE INTO swap_events (
       tx_hash, block_number, timestamp, wallet_address, dex_address, dex_name,
       token_in_address, token_in_symbol, token_in_amount,
       token_out_address, token_out_symbol, token_out_amount,
       value_usd, pnl, is_win
     )
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      swapEvent.tx_hash,
      swapEvent.block_number,
      swapEvent.timestamp,
      walletAddress,
      swapEvent.dex_address,
      swapEvent.dex_name || 'Unknown DEX',
      swapEvent.token_in_address,
      tokenInSymbol,
      swapEvent.token_in_amount,
      swapEvent.token_out_address,
      tokenOutSymbol,
      swapEvent.token_out_amount,
      valueInUsd,
      pnl,
      isWin === null ? null : (isWin ? 1 : 0),
    ]
  );
}

/**
 * Processa um bloco completo
 */
async function processBlock(blockNumber: number): Promise<void> {
  const provider = getProvider();
  const block = await getBlockWithTransactions(blockNumber);

  if (!block) {
    logger.warn(`Block ${blockNumber} not found`);
    return;
  }

  const transactions = block.prefetchedTransactions as TransactionResponse[];

  logger.debug(`Processing block ${blockNumber} with ${transactions.length} transactions`);

  // Processar transações em batches para não sobrecarregar o RPC
  const batches = chunkArray(transactions, 10);

  for (const batch of batches) {
    await Promise.allSettled(
      batch.map((tx) => processTransaction(tx, block, provider))
    );
  }
}

/**
 * Inicia o indexador em modo de sincronização histórica
 */
async function syncHistoricalBlocks(
  fromBlock: number,
  toBlock: number
): Promise<void> {
  logger.info(`Starting historical sync from block ${fromBlock} to ${toBlock}`);
  const totalBlocks = toBlock - fromBlock;
  let processedBlocks = 0;
  const startTime = Date.now();

  for (let blockNum = fromBlock; blockNum <= toBlock; blockNum += config.blockchain.batchSize) {
    const endBlock = Math.min(blockNum + config.blockchain.batchSize - 1, toBlock);

    // Processar blocos em paralelo (batch)
    const blockNumbers = Array.from(
      { length: endBlock - blockNum + 1 },
      (_, i) => blockNum + i
    );

    await Promise.allSettled(blockNumbers.map((bn) => processBlock(bn)));

    processedBlocks += blockNumbers.length;
    lastIndexedBlock = endBlock;

    const progress = (processedBlocks / totalBlocks) * 100;
    const elapsed = (Date.now() - startTime) / 1000;
    const blocksPerSecond = processedBlocks / elapsed;

    await updateIndexerState(endBlock, true, progress);

    logger.info(`Sync progress: ${progress.toFixed(2)}% (${processedBlocks}/${totalBlocks} blocks, ${blocksPerSecond.toFixed(2)} blocks/s)`);

    // Pequena pausa para não sobrecarregar o RPC
    await sleep(100);
  }
}

/**
 * Inicia o indexador em modo de tempo real (polling)
 */
async function startRealtimeIndexing(): Promise<void> {
  logger.info('Starting real-time indexing...');

  while (isRunning) {
    try {
      const latestBlock = await getLatestBlockNumber();

      if (latestBlock > lastIndexedBlock) {
        const blocksToProcess = Math.min(
          latestBlock - lastIndexedBlock,
          config.blockchain.batchSize
        );

        for (let i = 1; i <= blocksToProcess; i++) {
          const blockNum = lastIndexedBlock + i;
          await processBlock(blockNum);
          lastIndexedBlock = blockNum;
          await updateIndexerState(blockNum, false);
        }
      }
    } catch (error) {
      logger.error('Error in real-time indexing', { error: (error as Error).message });
    }

    await sleep(config.blockchain.pollingInterval);
  }
}

/**
 * Inicia o indexador completo
 */
export async function startIndexer(): Promise<void> {
  if (isRunning) {
    logger.warn('Indexer is already running');
    return;
  }

  isRunning = true;
  logger.info('Starting blockchain indexer for Base...');

  try {
    // Obter último bloco indexado
    lastIndexedBlock = await getLastIndexedBlock();
    const latestBlock = await getLatestBlockNumber();

    logger.info(`Last indexed block: ${lastIndexedBlock}, Latest block: ${latestBlock}`);

    // Se há blocos históricos para sincronizar
    if (lastIndexedBlock < latestBlock) {
      const startBlock = lastIndexedBlock === 0
        ? Math.max(latestBlock - 10000, 0) // Começar dos últimos 10000 blocos se for primeira vez
        : lastIndexedBlock + 1;

      await syncHistoricalBlocks(startBlock, latestBlock);
    }

    // Iniciar indexação em tempo real
    await startRealtimeIndexing();
  } catch (error) {
    logger.error('Indexer error', { error: (error as Error).message });
    isRunning = false;
    throw error;
  }
}

/**
 * Para o indexador
 */
export function stopIndexer(): void {
  isRunning = false;
  logger.info('Indexer stopped');
}

/**
 * Obtém o status atual do indexador
 */
export async function getIndexerStatus(): Promise<IndexerStatus> {
  const latestBlock = await getLatestBlockNumber();

  return {
    isRunning,
    lastIndexedBlock,
    latestBlock,
    syncProgress:
      latestBlock > 0 ? (lastIndexedBlock / latestBlock) * 100 : 0,
    blocksPerSecond: 0, // TODO: calcular em tempo real
  };
}
