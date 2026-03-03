import { query, execute } from './connection';
import { logger } from '../utils/logger';

// Cada migration é executada como statements separados
// pois o mysql2 não suporta múltiplos statements em uma única chamada execute()
const migrations: Array<{
  version: number;
  name: string;
  statements: string[];
}> = [
  {
    version: 1,
    name: 'create_initial_tables',
    statements: [
      // Tabela de controle de migrations (criada primeiro)
      `CREATE TABLE IF NOT EXISTS schema_migrations (
        version INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

      // Tabela de carteiras rastreadas
      `CREATE TABLE IF NOT EXISTS wallets (
        address VARCHAR(42) NOT NULL,
        label VARCHAR(100) DEFAULT NULL,
        first_seen DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_seen DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        total_transactions INT NOT NULL DEFAULT 0,
        is_contract TINYINT(1) NOT NULL DEFAULT 0,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (address)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

      // Tabela de transações indexadas
      `CREATE TABLE IF NOT EXISTS transactions (
        hash VARCHAR(66) NOT NULL,
        wallet_address VARCHAR(42) NOT NULL,
        block_number BIGINT NOT NULL,
        timestamp DATETIME NOT NULL,
        from_address VARCHAR(42) NOT NULL,
        to_address VARCHAR(42) DEFAULT NULL,
        value_eth DECIMAL(36,18) NOT NULL DEFAULT 0,
        value_usd DECIMAL(20,6) DEFAULT NULL,
        token_address VARCHAR(42) DEFAULT NULL,
        token_symbol VARCHAR(20) DEFAULT NULL,
        token_decimals INT DEFAULT NULL,
        token_amount DECIMAL(65,0) DEFAULT NULL,
        gas_used BIGINT DEFAULT NULL,
        gas_price BIGINT DEFAULT NULL,
        tx_type VARCHAR(20) NOT NULL DEFAULT 'other',
        is_win TINYINT(1) DEFAULT NULL,
        pnl DECIMAL(20,6) DEFAULT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (hash),
        CONSTRAINT fk_tx_wallet FOREIGN KEY (wallet_address) REFERENCES wallets(address)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

      // Tabela de eventos de swap
      `CREATE TABLE IF NOT EXISTS swap_events (
        id BIGINT NOT NULL AUTO_INCREMENT,
        tx_hash VARCHAR(66) NOT NULL,
        block_number BIGINT NOT NULL,
        timestamp DATETIME NOT NULL,
        wallet_address VARCHAR(42) NOT NULL,
        dex_address VARCHAR(42) NOT NULL,
        dex_name VARCHAR(50) DEFAULT NULL,
        token_in_address VARCHAR(42) NOT NULL,
        token_in_symbol VARCHAR(20) DEFAULT NULL,
        token_in_amount DECIMAL(65,0) NOT NULL,
        token_out_address VARCHAR(42) NOT NULL,
        token_out_symbol VARCHAR(20) DEFAULT NULL,
        token_out_amount DECIMAL(65,0) NOT NULL,
        value_usd DECIMAL(20,6) DEFAULT NULL,
        pnl DECIMAL(20,6) DEFAULT NULL,
        is_win TINYINT(1) DEFAULT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uq_swap_tx_dex (tx_hash, dex_address),
        CONSTRAINT fk_swap_wallet FOREIGN KEY (wallet_address) REFERENCES wallets(address)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

      // Tabela de informações de tokens
      `CREATE TABLE IF NOT EXISTS tokens (
        address VARCHAR(42) NOT NULL,
        symbol VARCHAR(20) DEFAULT NULL,
        name VARCHAR(100) DEFAULT NULL,
        decimals INT NOT NULL DEFAULT 18,
        price_usd DECIMAL(30,10) DEFAULT NULL,
        price_updated_at DATETIME DEFAULT NULL,
        is_verified TINYINT(1) NOT NULL DEFAULT 0,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (address)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

      // Tabela de métricas dos KOLs
      `CREATE TABLE IF NOT EXISTS kol_metrics (
        id BIGINT NOT NULL AUTO_INCREMENT,
        wallet_address VARCHAR(42) NOT NULL,
        period VARCHAR(20) NOT NULL,
        period_start DATETIME NOT NULL,
        period_end DATETIME NOT NULL,
        wins INT NOT NULL DEFAULT 0,
        losses INT NOT NULL DEFAULT 0,
        total_trades INT NOT NULL DEFAULT 0,
        profit_eth DECIMAL(36,18) NOT NULL DEFAULT 0,
        profit_usd DECIMAL(20,6) NOT NULL DEFAULT 0,
        win_rate DECIMAL(5,2) NOT NULL DEFAULT 0,
        avg_trade_size_eth DECIMAL(36,18) NOT NULL DEFAULT 0,
        best_trade_pnl DECIMAL(20,6) DEFAULT NULL,
        worst_trade_pnl DECIMAL(20,6) DEFAULT NULL,
        unique_tokens_traded INT NOT NULL DEFAULT 0,
        last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uq_kol_period (wallet_address, period, period_start),
        CONSTRAINT fk_kol_wallet FOREIGN KEY (wallet_address) REFERENCES wallets(address)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

      // Tabela de estado do indexador
      `CREATE TABLE IF NOT EXISTS indexer_state (
        id INT NOT NULL DEFAULT 1,
        last_indexed_block BIGINT NOT NULL DEFAULT 0,
        last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        is_syncing TINYINT(1) NOT NULL DEFAULT 0,
        sync_progress DECIMAL(5,2) DEFAULT NULL,
        PRIMARY KEY (id),
        CONSTRAINT chk_singleton CHECK (id = 1)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

      // Inserir estado inicial do indexador
      `INSERT IGNORE INTO indexer_state (id, last_indexed_block) VALUES (1, 0)`,
    ],
  },
  {
    version: 2,
    name: 'create_indexes',
    statements: [
      `CREATE INDEX IF NOT EXISTS idx_transactions_wallet    ON transactions(wallet_address)`,
      `CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp DESC)`,
      `CREATE INDEX IF NOT EXISTS idx_transactions_block     ON transactions(block_number DESC)`,
      `CREATE INDEX IF NOT EXISTS idx_transactions_type      ON transactions(tx_type)`,
      `CREATE INDEX IF NOT EXISTS idx_swap_events_wallet     ON swap_events(wallet_address)`,
      `CREATE INDEX IF NOT EXISTS idx_swap_events_timestamp  ON swap_events(timestamp DESC)`,
      `CREATE INDEX IF NOT EXISTS idx_swap_events_token_in   ON swap_events(token_in_address)`,
      `CREATE INDEX IF NOT EXISTS idx_swap_events_token_out  ON swap_events(token_out_address)`,
      `CREATE INDEX IF NOT EXISTS idx_kol_metrics_wallet     ON kol_metrics(wallet_address)`,
      `CREATE INDEX IF NOT EXISTS idx_kol_metrics_period     ON kol_metrics(period, period_start)`,
      `CREATE INDEX IF NOT EXISTS idx_kol_metrics_profit     ON kol_metrics(profit_usd DESC)`,
      `CREATE INDEX IF NOT EXISTS idx_wallets_last_seen      ON wallets(last_seen DESC)`,
    ],
  },
];

export async function runMigrations(): Promise<void> {
  logger.info('Running database migrations...');

  // Garantir que a tabela de controle existe antes de tudo
  await execute(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version INT PRIMARY KEY,
      name VARCHAR(100) NOT NULL,
      applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  // Buscar migrations já aplicadas
  const applied = await query<{ version: number }>('SELECT version FROM schema_migrations');
  const appliedVersions = new Set(applied.map((r) => r.version));

  // Aplicar migrations pendentes em ordem
  for (const migration of migrations) {
    if (appliedVersions.has(migration.version)) continue;

    logger.info(`Applying migration ${migration.version}: ${migration.name}`);

    try {
      for (const stmt of migration.statements) {
        await execute(stmt);
      }

      await execute(
        'INSERT INTO schema_migrations (version, name) VALUES (?, ?)',
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

  logger.info('All migrations applied successfully');
}
