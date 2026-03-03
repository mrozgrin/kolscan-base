// ─────────────────────────────────────────────────────────────────────────────
// Tipos base
// ─────────────────────────────────────────────────────────────────────────────

export interface Wallet {
  address: string;
  label?: string | null;
  first_seen: Date;
  last_seen: Date;
  total_transactions: number;
  is_disqualified?: boolean;
  flag_scalper?: boolean;
  flag_bundler?: boolean;
  flag_creator_funded?: boolean;
  flag_sybil?: boolean;
  scalper_copiability_index?: number | null;
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
  pnl?: number;
  is_win?: boolean;
  holding_time_s?: number | null;
  is_long_trade?: number | null;
}

export interface TokenInfo {
  address: string;
  symbol: string;
  name: string;
  decimals: number;
  price_usd?: number;
  last_updated?: Date;
}

// ─────────────────────────────────────────────────────────────────────────────
// Métricas
// ─────────────────────────────────────────────────────────────────────────────

export interface KolMetrics {
  wallet_address: string;
  period: 'daily' | 'weekly' | 'monthly' | 'all_time';
  wins: number;
  losses: number;
  total_trades: number;
  profit_usd: number;
  win_rate: number;
  best_trade_pnl?: number;
  worst_trade_pnl?: number;
  unique_tokens_traded?: number;
  last_updated: Date;
}

// Componentes do Follow Score v2
export interface ScoreComponentsDetail {
  followability: {
    final: number;
    hold_score: number;
    volume_score: number;
    liq_score: number;
  };
  consistency: {
    final: number;
    wr_stability: number;
    pnl_stability: number;
    diversification: number;
  };
  pnl: {
    final: number;
    relative_score: number;
    profit_factor: number;
    pf_score: number;
  };
  win_rate: {
    score: number;
    raw_pct: number;
  };
}

export interface PeriodMetrics {
  period: string;
  wins: number;
  losses: number;
  total_trades: number;
  profit_usd: number;
  profit_pct: number;
  win_rate: number;
  follow_score: number;
  follow_score_label: string;
  has_sufficient_data: boolean;
  score_components: ScoreComponentsDetail;
  aux_metrics: {
    trades_per_day: number;
    diversification_ratio: number;
    gross_profit_usd: number;
    gross_loss_usd: number;
    monthly_wr_cv: number;
    profitable_months_ratio: number;
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Leaderboard
// ─────────────────────────────────────────────────────────────────────────────

export interface LeaderboardEntry {
  rank: number;
  wallet_address: string;
  label?: string | null;
  wins: number;
  losses: number;
  total_trades: number;
  profit_usd: number;
  profit_pct: number;
  win_rate: number;
  period: string;
  holding_time_avg_s: number;
  holding_time_formatted: string | null;
  scalping_rate: number;
  long_trade_rate_pct: number;
  follow_score: number;
  follow_score_label: string;
  score_components: {
    followability: number;
    consistency: number;
    pnl: number;
    win_rate: number;
  };
  trades_per_day: number;
  diversification_ratio: number;
}

// ─────────────────────────────────────────────────────────────────────────────
// Detalhes do KOL
// ─────────────────────────────────────────────────────────────────────────────

export interface KolDetails {
  wallet_address: string;
  label?: string | null;
  first_seen: Date;
  last_seen: Date;
  total_transactions: number;
  is_disqualified: boolean;
  disqualification_reason: string | null;
  flags: {
    scalper: boolean;
    bundler: boolean;
    creator_funded: boolean;
    sybil: boolean;
  };
  copiability_index: number | null;
  copiability_pct: number | null;
  holding_analysis: {
    scalping_threshold_s: number;
    scalping_threshold_formatted: string | null;
    avg_holding_s: number | null;
    avg_holding_formatted: string | null;
    long_trade_rate_pct: number | null;
    scalping_rate_pct: number;
    follow_score: number;
    follow_score_label: string;
  };
  metrics_by_period: PeriodMetrics[];
  recent_swaps: Array<{
    tx_hash: string;
    timestamp: Date;
    dex_name: string;
    token_in_symbol: string;
    token_out_symbol: string;
    token_in_address: string;
    token_out_address: string;
    value_usd: number;
    pnl: number;
    is_win: boolean;
    holding_time_s: number | null;
    holding_time_formatted: string | null;
    is_long_trade: number | null;
    /** PnL real na moeda base (ex: WETH). Disponível apenas em vendas (swap_type='sell'). */
    pnl_base: number | null;
    /** Símbolo da moeda base em que o PnL foi registrado (ex: 'WETH', 'USDC'). */
    pnl_base_symbol: string | null;
    /** Tipo do swap: 'buy' (compra), 'sell' (venda) ou 'swap' (meme→meme). */
    swap_type: string | null;
  }>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Infraestrutura
// ─────────────────────────────────────────────────────────────────────────────

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

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
  pagination?: {
    page: number;
    limit: number;
    total?: number;
    total_pages?: number;
    count?: number;
  };
}
