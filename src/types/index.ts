export interface Wallet {
  address: string;
  first_seen: Date;
  last_seen: Date;
  total_transactions: number;
  label?: string;
}

export interface Transaction {
  hash: string;
  wallet_address: string;
  block_number: number;
  timestamp: Date;
  from_address: string;
  to_address: string;
  value_eth: string;
  value_usd?: number;
  token_address?: string;
  token_symbol?: string;
  token_decimals?: number;
  token_amount?: string;
  gas_used?: string;
  gas_price?: string;
  tx_type: 'swap' | 'transfer' | 'contract_call' | 'other';
  is_win?: boolean;
  pnl?: number;
}

export interface KolMetrics {
  wallet_address: string;
  period: 'daily' | 'weekly' | 'monthly' | 'all_time';
  wins: number;
  losses: number;
  total_trades: number;
  profit_eth: number;
  profit_usd: number;
  win_rate: number;
  avg_trade_size_eth: number;
  best_trade_pnl?: number;
  worst_trade_pnl?: number;
  unique_tokens_traded: number;
  last_updated: Date;
}

export interface LeaderboardEntry {
  rank: number;
  wallet_address: string;
  label?: string;
  wins: number;
  losses: number;
  total_trades: number;
  profit_eth: number;
  profit_usd: number;
  win_rate: number;
  period: 'daily' | 'weekly' | 'monthly' | 'all_time';
  // Métricas de holding time e follow score
  holding_time_avg_s?: number;    // Média de segundos nas posições
  scalping_rate: number;          // % de trades considerados scalping
  follow_score: number;           // Nota 0-100 para seguir o trader
}

export interface TokenInfo {
  address: string;
  symbol: string;
  name: string;
  decimals: number;
  price_usd?: number;
  last_updated?: Date;
}

export interface BlockchainConfig {
  rpc_url: string;
  chain_id: number;
  chain_name: string;
  start_block?: number;
}

export interface IndexerState {
  last_indexed_block: number;
  last_updated: Date;
  is_syncing: boolean;
  sync_progress?: number;
}

export interface SwapEvent {
  tx_hash: string;
  block_number: number;
  timestamp: Date;
  wallet_address: string;
  dex_address: string;
  dex_name?: string;
  token_in_address: string;
  token_in_symbol: string;
  token_in_amount: string;
  token_out_address: string;
  token_out_symbol: string;
  token_out_amount: string;
  value_usd?: number;
}

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
  pagination?: {
    page: number;
    limit: number;
    total: number;
    total_pages: number;
  };
}
