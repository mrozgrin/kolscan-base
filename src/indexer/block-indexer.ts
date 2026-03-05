import { ethers } from 'ethers';
import type { Block, TransactionResponse, Log } from 'ethers';
import Decimal from 'decimal.js';
import { getProvider, getBlockWithTransactions, getLatestBlockNumber } from './provider';
import { extractSwapEvents, SWAP_TOPICS } from './dex-decoder';
import { query, execute } from '../database/connection';
// getTokenPrice and getTokenInfo are used indirectly via processSwapEvent
import { getTokenPrice, getTokenInfo } from '../services/price-service';
import { config } from '../config';
import { logger } from '../utils/logger';
import { sleep, chunkArray } from '../utils/helpers';
import { SwapEvent } from '../types';

// Precisão global para todos os cálculos de posição e PnL
Decimal.set({ precision: 36, rounding: Decimal.ROUND_DOWN });

const ZERO = new Decimal(0);

interface IndexerStatus {
  isRunning: boolean;
  lastIndexedBlock: number;
  latestBlock: number;
  syncProgress: number;
  blocksPerSecond: number;
}

let isRunning = false;
let lastIndexedBlock = 0;

// Fila de blocos que falharam e precisam ser reprocessados
const pendingRetryBlocks = new Set<number>();

// Máximo de tentativas antes de confirmar que o bloco não existe na blockchain
const MAX_BLOCK_RETRIES = 5;
// Delay base para backoff exponencial (ms)
const RETRY_BASE_DELAY_MS = 1000;

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

// Tokens considerados "moeda base" — usados como referência de valor nas posições.
// Quando token_in é um desses, o swap é uma COMPRA do token_out.
// Quando token_out é um desses, o swap é uma VENDA do token_in.
const BASE_TOKENS = new Set([
  '0x4200000000000000000000000000000000000006', // WETH (Base)
  '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913', // USDC (Base)
  '0xfde4c96c8593536e31f229ea8f37b2ada2699bb2', // USDT (Base)
  '0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca', // USDbC (Base)
]);

/**
 * Calcula o tempo de holding em segundos para um token.
 * Busca a compra mais recente do mesmo token pela mesma carteira
 * e retorna a diferença de tempo em segundos.
 * Retorna null se não houver compra anterior registrada.
 */
async function calculateHoldingTime(
  walletAddress: string,
  tokenSoldAddress: string, // token que está sendo vendido agora
  currentTimestamp: Date
): Promise<number | null> {
  try {
    // Buscar o swap mais recente onde este token foi comprado (swap_type='buy')
    const result = await query<{ timestamp: Date }>(
      `SELECT timestamp
       FROM swap_events
       WHERE wallet_address    = ?
         AND token_out_address = ?
         AND swap_type         = 'buy'
         AND timestamp        < ?
       ORDER BY timestamp DESC
       LIMIT 1`,
      [walletAddress, tokenSoldAddress, currentTimestamp]
    );

    if (!result.length) return null;

    const buyTime = new Date(result[0].timestamp).getTime();
    const sellTime = currentTimestamp.getTime();
    const holdingSeconds = Math.round((sellTime - buyTime) / 1000);

    // Sanidade: ignorar valores negativos ou absurdamente grandes (> 1 ano)
    if (holdingSeconds <= 0 || holdingSeconds > 365 * 24 * 3600) return null;

    return holdingSeconds;
  } catch {
    return null;
  }
}

/**
 * Atualiza (ou cria) a posição aberta de uma wallet para um token.
 * Retorna o custo médio por unidade na moeda base ANTES da atualização
 * (necessário para calcular o PnL proporcional em vendas parciais).
 *
 * @param walletAddress  Endereço da wallet
 * @param tokenAddress   Token da posição (ex: XYZCOIN)
 * @param baseAddress    Moeda base (ex: WETH)
 * @param baseSymbol     Símbolo da moeda base
 * @param qtyDelta       +quantidade (compra) ou -quantidade (venda)
 * @param costDelta      +custo em base (compra) ou 0 (venda — custo é deduzido proporcionalmente)
 * @param openedAt       Timestamp da abertura (usado apenas na criação da posição)
 * @returns              { avgCostBefore, costBasisBefore } — valores ANTES da atualização
 */
async function updatePosition(
  walletAddress: string,
  tokenAddress: string,
  baseAddress: string,
  baseSymbol: string,
  qtyDelta: number,
  costDelta: number,
  openedAt: Date
): Promise<{ avgCostBefore: number; costBasisBefore: number }> {
  try {
    // Buscar posição atual
    const rows = await query<{ qty_open: string; cost_basis_base: string; avg_cost_base: string }>(
      `SELECT qty_open, cost_basis_base, avg_cost_base
       FROM positions
       WHERE wallet_address = ? AND token_address = ?`,
      [walletAddress, tokenAddress]
    );

    const qtyBefore      = rows.length ? parseFloat(rows[0].qty_open)        : 0;
    const costBefore     = rows.length ? parseFloat(rows[0].cost_basis_base) : 0;
    const avgCostBefore  = rows.length ? parseFloat(rows[0].avg_cost_base)   : 0;

    const qtyAfter  = Math.max(0, qtyBefore  + qtyDelta);
    const costAfter = qtyDelta > 0
      // Compra: acumula custo
      ? costBefore + costDelta
      // Venda: reduz custo proporcionalmente ao percentual vendido
      : qtyBefore > 0 ? costBefore * (qtyAfter / qtyBefore) : 0;

    const avgCostAfter = qtyAfter > 0 ? costAfter / qtyAfter : 0;

    if (rows.length === 0) {
      // Criar nova posição
      await execute(
        `INSERT INTO positions
           (wallet_address, token_address, base_token_address, base_token_symbol,
            qty_open, cost_basis_base, avg_cost_base, opened_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [walletAddress, tokenAddress, baseAddress, baseSymbol,
         qtyAfter, costAfter, avgCostAfter, openedAt]
      );
    } else {
      // Atualizar posição existente
      await execute(
        `UPDATE positions
         SET qty_open = ?, cost_basis_base = ?, avg_cost_base = ?,
             base_token_address = ?, base_token_symbol = ?, last_updated = NOW()
         WHERE wallet_address = ? AND token_address = ?`,
        [qtyAfter, costAfter, avgCostAfter,
         baseAddress, baseSymbol,
         walletAddress, tokenAddress]
      );
    }

    return { avgCostBefore, costBasisBefore: costBefore };
  } catch (err) {
    // Erro ao atualizar posição não deve travar o indexador.
    // Registrar e retornar zeros — o swap ainda será gravado sem PnL.
    logger.warn('updatePosition failed, skipping position update', {
      walletAddress,
      tokenAddress,
      error: (err as Error).message,
    });
    return { avgCostBefore: 0, costBasisBefore: 0 };
  }
}

/**
 * Processa um evento de swap, calcula PnL por posição na moeda base e holding time.
 *
 * Lógica de classificação:
 *   BUY  — token_in é moeda base (ex: WETH → XYZCOIN)
 *          Abre/aumenta posição. PnL = null.
 *
 *   SELL — token_out é moeda base (ex: XYZCOIN → WETH)
 *          Fecha/reduz posição.
 *          pnl_base = valor_recebido_base − (avg_cost_base × qty_vendida)
 *
 *   SWAP — nenhum dos lados é moeda base (ex: XYZCOIN → ABCCOIN)
 *          Fecha posição do token_in (sem PnL em base) e abre posição do token_out.
 *          pnl_base = null (não há moeda base para registrar o lucro).
 */
async function processSwapEvent(
  swapEvent: SwapEvent,
  walletAddress: string
): Promise<void> {
  try {
  // ── Informações e decimais dos tokens ──────────────────────────────────────
  const [tokenInInfo, tokenOutInfo] = await Promise.all([
    getTokenInfo(swapEvent.token_in_address),
    getTokenInfo(swapEvent.token_out_address),
  ]);

  const tokenInSymbol    = tokenInInfo?.symbol   || 'UNKNOWN';
  const tokenOutSymbol   = tokenOutInfo?.symbol  || 'UNKNOWN';
  const tokenInDecimals  = tokenInInfo?.decimals  || 18;
  const tokenOutDecimals = tokenOutInfo?.decimals || 18;

  const tokenInAmount  = parseFloat(swapEvent.token_in_amount)  / Math.pow(10, tokenInDecimals);
  const tokenOutAmount = parseFloat(swapEvent.token_out_amount) / Math.pow(10, tokenOutDecimals);

  // ── Preços em USD (mantidos para fins de leaderboard/métricas) ─────────────
  const [tokenInPrice, tokenOutPrice] = await Promise.all([
    getTokenPrice(swapEvent.token_in_address),
    getTokenPrice(swapEvent.token_out_address),
  ]);

  const valueInUsd  = tokenInPrice  ? tokenInAmount  * tokenInPrice  : null;
  const valueOutUsd = tokenOutPrice ? tokenOutAmount * tokenOutPrice : null;
  // value_usd representa o valor de entrada (custo) em USD
  const valueUsd = valueInUsd;

  // ── Classificar o tipo de swap ─────────────────────────────────────────────
  const inIsBase  = BASE_TOKENS.has(swapEvent.token_in_address.toLowerCase());
  const outIsBase = BASE_TOKENS.has(swapEvent.token_out_address.toLowerCase());

  let swapType: 'buy' | 'sell' | 'swap';
  if (inIsBase && !outIsBase)       swapType = 'buy';
  else if (!inIsBase && outIsBase)  swapType = 'sell';
  else                              swapType = 'swap'; // base→base ou meme→meme

  // ── PnL na moeda base ──────────────────────────────────────────────────────
  let pnlBase:       number | null = null;
  let pnlBaseToken:  string | null = null;
  let pnlBaseSymbol: string | null = null;
  // pnl em USD (legado — mantido para compatibilidade com métricas existentes)
  let pnl:    number | null = null;
  let isWin:  boolean | null = null;

  if (swapType === 'buy') {
    // ── COMPRA: abre/aumenta posição ────────────────────────────────────────
    // Custo de entrada = quantidade de moeda base gasta (token_in_amount)
    await updatePosition(
      walletAddress,
      swapEvent.token_out_address,   // token que está sendo comprado
      swapEvent.token_in_address,    // moeda base usada para comprar
      tokenInSymbol,
      tokenOutAmount,                // qty comprada do token_out
      tokenInAmount,                 // custo em moeda base
      swapEvent.timestamp
    );
    // PnL é null em compras — posição ainda aberta

  } else if (swapType === 'sell') {
    // ── VENDA: fecha/reduz posição e calcula PnL na moeda base ──────────────
    const { avgCostBefore } = await updatePosition(
      walletAddress,
      swapEvent.token_in_address,    // token que está sendo vendido
      swapEvent.token_out_address,   // moeda base recebida
      tokenOutSymbol,
      -tokenInAmount,                // qty vendida (negativo = redução)
      0,                             // custo deduzido proporcionalmente dentro de updatePosition
      swapEvent.timestamp
    );

    // PnL na moeda base:
    //   valor recebido (token_out_amount em base) − custo proporcional (avg_cost × qty_vendida)
    const costProporcional = avgCostBefore * tokenInAmount;
    pnlBase       = tokenOutAmount - costProporcional;
    pnlBaseToken  = swapEvent.token_out_address;
    pnlBaseSymbol = tokenOutSymbol;
    isWin         = pnlBase > 0;

    // PnL em USD (para métricas de leaderboard)
    if (valueInUsd !== null && valueOutUsd !== null) {
      pnl = valueOutUsd - (avgCostBefore * tokenInAmount * (tokenInPrice || 0) / (tokenInPrice || 1));
      // Simplificação: se temos preço de ambos, usa a diferença USD direta ponderada pelo custo médio
      // Para o leaderboard, o sinal de is_win já vem do pnl_base
      pnl = valueOutUsd - (costProporcional * (tokenInPrice || tokenOutPrice || 1));
    }

  } else {
    // ── SWAP meme→meme: fecha posição do token_in sem PnL em base ───────────
    // Fecha posição do token vendido (sem base para registrar lucro)
    await updatePosition(
      walletAddress,
      swapEvent.token_in_address,
      swapEvent.token_in_address, // placeholder — não há base real
      tokenInSymbol,
      -tokenInAmount,
      0,
      swapEvent.timestamp
    );
    // Abre posição do token comprado usando valor USD como proxy de custo
    if (valueInUsd !== null) {
      await updatePosition(
        walletAddress,
        swapEvent.token_out_address,
        swapEvent.token_in_address, // moeda "base" é o token vendido (proxy)
        tokenInSymbol,
        tokenOutAmount,
        valueInUsd,
        swapEvent.timestamp
      );
    }
  }

  // ── Holding time ───────────────────────────────────────────────────────────
  // Calculado apenas em vendas (quando o token_in foi previamente comprado)
  const holdingTimeS = (swapType === 'sell' || swapType === 'swap')
    ? await calculateHoldingTime(walletAddress, swapEvent.token_in_address, swapEvent.timestamp)
    : null;

  const scalpingThreshold = config.indexer.scalpingThresholdSeconds;
  const isLongTrade = holdingTimeS === null
    ? null
    : holdingTimeS >= scalpingThreshold ? 1 : 0;

  // ── Log visual do swap ─────────────────────────────────────────────────────
  const pnlStr = pnlBase !== null
    ? ` | PnL: ${pnlBase >= 0 ? '+' : ''}${pnlBase.toFixed(6)} ${pnlBaseSymbol} (${isWin ? '✓' : '✗'})`
    : '';
  logger.info(
    `  [${swapType.toUpperCase().padEnd(4)}] ${walletAddress.substring(0, 8)}... ` +
    `${tokenInAmount.toFixed(4)} ${tokenInSymbol} → ${tokenOutAmount.toFixed(4)} ${tokenOutSymbol}` +
    `${pnlStr} → MySQL ✓`
  );

  // ── Salvar evento de swap ──────────────────────────────────────────────────
  await execute(
    `INSERT IGNORE INTO swap_events (
       tx_hash, block_number, timestamp, wallet_address, dex_address, dex_name,
       token_in_address, token_in_symbol, token_in_amount,
       token_out_address, token_out_symbol, token_out_amount,
       value_usd, pnl, is_win, holding_time_s, is_long_trade,
       pnl_base, pnl_base_token, pnl_base_symbol, swap_type
     )
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
      valueUsd,
      pnl,
      isWin === null ? null : (isWin ? 1 : 0),
      holdingTimeS,
      isLongTrade,
      pnlBase,
      pnlBaseToken,
      pnlBaseSymbol,
      swapType,
    ]
  );
  } catch (err) {
    // Erro em processSwapEvent não deve travar o indexador.
    // O bloco continua sendo processado normalmente.
    logger.warn('processSwapEvent failed, swap skipped', {
      tx_hash: swapEvent.tx_hash,
      wallet: walletAddress,
      error: (err as Error).message,
    });
  }
}

/**
 * Busca um bloco com retry e backoff exponencial.
 * Só desiste após MAX_BLOCK_RETRIES tentativas — nunca ignora silenciosamente.
 * Retorna null apenas se o bloco confirmadamente não existe na blockchain
 * (número além do head da rede após todas as tentativas).
 */
async function fetchBlockWithRetry(
  blockNumber: number
): Promise<Awaited<ReturnType<typeof getBlockWithTransactions>>> {
  for (let attempt = 1; attempt <= MAX_BLOCK_RETRIES; attempt++) {
    try {
      const block = await getBlockWithTransactions(blockNumber);
      if (block) return block;

      // Bloco retornou null — pode ser lag do RPC ou bloco ainda não propagado.
      // Verificar se o número já existe na rede antes de desistir.
      const networkHead = await getLatestBlockNumber();

      if (blockNumber > networkHead) {
        // Bloco genuinamente não existe ainda — aguardar mineração
        logger.debug(
          `Block ${blockNumber} not yet mined (head: ${networkHead}), waiting...`
        );
        await sleep(config.blockchain.pollingInterval);
        // Não conta como tentativa falha — é só espera
        attempt--;
        continue;
      }

      // Bloco deveria existir mas o RPC retornou null — pode ser lag de propagação
      const delay = RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1); // 1s, 2s, 4s, 8s, 16s
      logger.warn(
        `Block ${blockNumber} not found by RPC (attempt ${attempt}/${MAX_BLOCK_RETRIES}), retrying in ${delay}ms...`
      );
      await sleep(delay);
    } catch (err) {
      const delay = RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1);
      logger.warn(
        `Error fetching block ${blockNumber} (attempt ${attempt}/${MAX_BLOCK_RETRIES}): ${
          (err as Error).message
        }, retrying in ${delay}ms...`
      );
      await sleep(delay);
    }
  }

  // Após todas as tentativas, verificar uma última vez se o bloco existe na rede.
  // Se não existir (ex: bloco órfão ou número inválido), registrar e pular.
  const networkHead = await getLatestBlockNumber();
  if (blockNumber > networkHead) {
    logger.warn(
      `Block ${blockNumber} does not exist on-chain (head: ${networkHead}). Skipping.`
    );
    return null;
  }

  // Bloco existe na rede mas o RPC não conseguiu retorná-lo após todas as tentativas.
  // Adicionar à fila de pendentes para reprocessamento posterior.
  logger.error(
    `Block ${blockNumber} could not be fetched after ${MAX_BLOCK_RETRIES} attempts. ` +
    `Added to pending retry queue.`
  );
  pendingRetryBlocks.add(blockNumber);
  return null;
}

/**
 * Reprocessa blocos que falharam anteriormente.
 * Chamado periodicamente durante a indexação em tempo real.
 */
async function retryPendingBlocks(): Promise<void> {
  if (pendingRetryBlocks.size === 0) return;

  logger.info(`Retrying ${pendingRetryBlocks.size} pending blocks...`);
  const toRetry = [...pendingRetryBlocks];

  for (const blockNumber of toRetry) {
    if (!isRunning) break;
    const block = await fetchBlockWithRetry(blockNumber);
    if (block) {
      // Sucesso — processar e remover da fila
      await processBlock(blockNumber);
      pendingRetryBlocks.delete(blockNumber);
      logger.info(`Pending block ${blockNumber} successfully reprocessed.`);
    }
  }
}

/**
 * Processa um bloco completo
 */
async function processBlock(blockNumber: number): Promise<void> {
  const provider = getProvider();
  const block = await fetchBlockWithRetry(blockNumber);

  if (!block) {
    // fetchBlockWithRetry já tratou o caso: ou adicionou à fila de pendentes
    // ou confirmou que o bloco não existe na blockchain.
    return;
  }

  const transactions = block.prefetchedTransactions as TransactionResponse[];

  const blockDate = new Date(Number(block.timestamp) * 1000).toISOString().replace('T', ' ').substring(0, 19);
  logger.debug(`Processing block ${blockNumber} with ${transactions.length} transactions`);

  // Processar transações em batches para não sobrecarregar o RPC
  const batches = chunkArray(transactions, 10);
  let swapCount = 0;

  for (const batch of batches) {
    const results = await Promise.allSettled(
      batch.map((tx) => processTransaction(tx, block, provider))
    );
    // Contar swaps processados com sucesso no batch
    swapCount += results.filter((r) => r.status === 'fulfilled').length;
  }

  if (swapCount > 0) {
    logger.info(
      `[BLOCK ${blockNumber}] ${blockDate} | ${swapCount} tx(s) processada(s) → MySQL ✓`
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
  const totalBlocks = Math.max(toBlock - fromBlock, 1);
  let processedBlocks = 0;
  const startTime = Date.now();

  for (let blockNum = fromBlock; blockNum <= toBlock; blockNum += config.blockchain.batchSize) {
    if (!isRunning) break;

    // Nunca ultrapassar o toBlock capturado no início da sincronização
    const endBlock = Math.min(blockNum + config.blockchain.batchSize - 1, toBlock);

    // Verificar o bloco mais recente da rede antes de cada batch:
    // se o endBlock ainda não foi minerado, aguardar até ele existir
    let networkHead = await getLatestBlockNumber();
    while (networkHead < endBlock) {
      logger.debug(`Waiting for block ${endBlock} to be mined (current head: ${networkHead})...`);
      await sleep(config.blockchain.pollingInterval);
      networkHead = await getLatestBlockNumber();
    }

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
    const eta = blocksPerSecond > 0
      ? Math.round((totalBlocks - processedBlocks) / blocksPerSecond)
      : null;
    const etaStr = eta !== null
      ? eta > 3600
        ? `${Math.floor(eta / 3600)}h${Math.floor((eta % 3600) / 60)}m`
        : eta > 60
          ? `${Math.floor(eta / 60)}m${eta % 60}s`
          : `${eta}s`
      : '?';

    await updateIndexerState(endBlock, true, progress);

    logger.info(
      `[SYNC] ${progress.toFixed(1)}% | bloco ${endBlock} | ` +
      `${blocksPerSecond.toFixed(1)} blocos/s | ETA: ${etaStr} | ` +
      `${processedBlocks}/${totalBlocks} blocos`
    );

    await sleep(100);
  }
}

// Contador de ciclos para disparar retryPendingBlocks a cada N ciclos
let realtimeCycleCount = 0;
const RETRY_PENDING_EVERY_N_CYCLES = 10; // a cada ~10 × pollingInterval

/**
 * Inicia o indexador em modo de tempo real (polling)
 */
async function startRealtimeIndexing(): Promise<void> {
  logger.info('Starting real-time indexing...');

  let realtimeSwapTotal = 0;
  let realtimeBlockTotal = 0;
  let lastHeartbeat = Date.now();
  const HEARTBEAT_INTERVAL_MS = 30_000; // log de "vivo" a cada 30s mesmo sem novos blocos

  while (isRunning) {
    try {
      const latestBlock = await getLatestBlockNumber();

      if (latestBlock > lastIndexedBlock) {
        const blocksToProcess = Math.min(
          latestBlock - lastIndexedBlock,
          config.blockchain.batchSize
        );

        logger.info(
          `[REALTIME] ${blocksToProcess} novo(s) bloco(s) detectado(s) ` +
          `(${lastIndexedBlock + 1} → ${lastIndexedBlock + blocksToProcess}) | ` +
          `head da rede: ${latestBlock}`
        );

        for (let i = 1; i <= blocksToProcess; i++) {
          const blockNum = lastIndexedBlock + i;
          await processBlock(blockNum);
          lastIndexedBlock = blockNum;
          await updateIndexerState(blockNum, false);
          realtimeBlockTotal++;
        }

        lastHeartbeat = Date.now();
      } else {
        // Nenhum bloco novo — emitir heartbeat periódico para confirmar que está vivo
        if (Date.now() - lastHeartbeat >= HEARTBEAT_INTERVAL_MS) {
          logger.info(
            `[REALTIME] aguardando novos blocos... | último bloco: ${lastIndexedBlock} | ` +
            `blocos processados nesta sessão: ${realtimeBlockTotal}`
          );
          lastHeartbeat = Date.now();
        }
      }

      // Periodicamente tentar reprocessar blocos que falharam
      realtimeCycleCount++;
      if (realtimeCycleCount % RETRY_PENDING_EVERY_N_CYCLES === 0) {
        await retryPendingBlocks();
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
