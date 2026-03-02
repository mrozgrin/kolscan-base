import { query } from './connection';
import { logger } from '../utils/logger';

const migrations = [
  {
    version: 1,
    name: 'create_initial_tables',
    sql: `
      -- Tabela de carteiras rastreadas
      CREATE TABLE IF NOT EXISTS wallets (
        address VARCHAR(42) PRIMARY KEY,
        label VARCHAR(100),
        first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        total_transactions INTEGER NOT NULL DEFAULT 0,
        is_contract BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      -- Tabela de transações indexadas
      CREATE TABLE IF NOT EXISTS transactions (
        hash VARCHAR(66) PRIMARY KEY,
        wallet_address VARCHAR(42) NOT NULL REFERENCES wallets(address),
        block_number BIGINT NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL,
        from_address VARCHAR(42) NOT NULL,
        to_address VARCHAR(42),
        value_eth NUMERIC(36, 18) NOT NULL DEFAULT 0,
        value_usd NUMERIC(20, 6),
        token_address VARCHAR(42),
        token_symbol VARCHAR(20),
        token_decimals INTEGER,
        token_amount NUMERIC(78, 0),
        gas_used BIGINT,
        gas_price BIGINT,
        tx_type VARCHAR(20) NOT NULL DEFAULT 'other',
        is_win BOOLEAN,
        pnl NUMERIC(20, 6),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      -- Tabela de eventos de swap
      CREATE TABLE IF NOT EXISTS swap_events (
        id SERIAL PRIMARY KEY,
        tx_hash VARCHAR(66) NOT NULL,
        block_number BIGINT NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL,
        wallet_address VARCHAR(42) NOT NULL REFERENCES wallets(address),
        dex_address VARCHAR(42) NOT NULL,
        dex_name VARCHAR(50),
        token_in_address VARCHAR(42) NOT NULL,
        token_in_symbol VARCHAR(20),
        token_in_amount NUMERIC(78, 0) NOT NULL,
        token_out_address VARCHAR(42) NOT NULL,
        token_out_symbol VARCHAR(20),
        token_out_amount NUMERIC(78, 0) NOT NULL,
        value_usd NUMERIC(20, 6),
        pnl NUMERIC(20, 6),
        is_win BOOLEAN,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(tx_hash, dex_address)
      );

      -- Tabela de informações de tokens
      CREATE TABLE IF NOT EXISTS tokens (
        address VARCHAR(42) PRIMARY KEY,
        symbol VARCHAR(20),
        name VARCHAR(100),
        decimals INTEGER NOT NULL DEFAULT 18,
        price_usd NUMERIC(20, 10),
        price_updated_at TIMESTAMPTZ,
        is_verified BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      -- Tabela de métricas dos KOLs
      CREATE TABLE IF NOT EXISTS kol_metrics (
        id SERIAL PRIMARY KEY,
        wallet_address VARCHAR(42) NOT NULL REFERENCES wallets(address),
        period VARCHAR(20) NOT NULL,
        period_start TIMESTAMPTZ NOT NULL,
        period_end TIMESTAMPTZ NOT NULL,
        wins INTEGER NOT NULL DEFAULT 0,
        losses INTEGER NOT NULL DEFAULT 0,
        total_trades INTEGER NOT NULL DEFAULT 0,
        profit_eth NUMERIC(36, 18) NOT NULL DEFAULT 0,
        profit_usd NUMERIC(20, 6) NOT NULL DEFAULT 0,
        win_rate NUMERIC(5, 2) NOT NULL DEFAULT 0,
        avg_trade_size_eth NUMERIC(36, 18) NOT NULL DEFAULT 0,
        best_trade_pnl NUMERIC(20, 6),
        worst_trade_pnl NUMERIC(20, 6),
        unique_tokens_traded INTEGER NOT NULL DEFAULT 0,
        last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(wallet_address, period, period_start)
      );

      -- Tabela de estado do indexador
      CREATE TABLE IF NOT EXISTS indexer_state (
        id INTEGER PRIMARY KEY DEFAULT 1,
        last_indexed_block BIGINT NOT NULL DEFAULT 0,
        last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        is_syncing BOOLEAN NOT NULL DEFAULT FALSE,
        sync_progress NUMERIC(5, 2),
        CHECK (id = 1)
      );

      -- Inserir estado inicial do indexador
      INSERT INTO indexer_state (id, last_indexed_block) 
      VALUES (1, 0) 
      ON CONFLICT (id) DO NOTHING;
    `,
  },
  {
    version: 2,
    name: 'create_indexes',
    sql: `
      -- Índices para melhorar performance das queries
      CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(wallet_address);
      CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp DESC);
      CREATE INDEX IF NOT EXISTS idx_transactions_block ON transactions(block_number DESC);
      CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(tx_type);
      CREATE INDEX IF NOT EXISTS idx_swap_events_wallet ON swap_events(wallet_address);
      CREATE INDEX IF NOT EXISTS idx_swap_events_timestamp ON swap_events(timestamp DESC);
      CREATE INDEX IF NOT EXISTS idx_swap_events_token_in ON swap_events(token_in_address);
      CREATE INDEX IF NOT EXISTS idx_swap_events_token_out ON swap_events(token_out_address);
      CREATE INDEX IF NOT EXISTS idx_kol_metrics_wallet ON kol_metrics(wallet_address);
      CREATE INDEX IF NOT EXISTS idx_kol_metrics_period ON kol_metrics(period, period_start);
      CREATE INDEX IF NOT EXISTS idx_kol_metrics_profit ON kol_metrics(profit_usd DESC);
      CREATE INDEX IF NOT EXISTS idx_wallets_last_seen ON wallets(last_seen DESC);
    `,
  },
  {
    version: 3,
    name: 'create_migrations_table',
    sql: `
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `,
  },
];

export async function runMigrations(): Promise<void> {
  logger.info('Running database migrations...');

  // Criar tabela de migrations se não existir
  await query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version INTEGER PRIMARY KEY,
      name VARCHAR(100) NOT NULL,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // Buscar migrations já aplicadas
  const applied = await query<{ version: number }>('SELECT version FROM schema_migrations');
  const appliedVersions = new Set(applied.map((r) => r.version));

  // Aplicar migrations pendentes
  for (const migration of migrations) {
    if (!appliedVersions.has(migration.version)) {
      logger.info(`Applying migration ${migration.version}: ${migration.name}`);

      try {
        await query(migration.sql);
        await query(
          'INSERT INTO schema_migrations (version, name) VALUES ($1, $2)',
          [migration.version, migration.name]
        );
        logger.info(`Migration ${migration.version} applied successfully`);
      } catch (error) {
        logger.error(`Migration ${migration.version} failed`, {
          error: (error as Error).message,
        });
        throw error;
      }
    }
  }

  logger.info('All migrations applied successfully');
}
