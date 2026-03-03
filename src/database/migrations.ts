import { query, execute, ddl } from './connection';
import { logger } from '../utils/logger';

// ─────────────────────────────────────────────────────────────────────────────
// Regras importantes para MySQL + mysql2:
//   • DDL (CREATE TABLE, CREATE INDEX, DROP, ALTER, CALL, CREATE PROCEDURE…)
//     → usar ddl() que usa conn.query() — NÃO suporta prepared statements
//   • DML com parâmetros (INSERT, UPDATE, DELETE, SELECT com ?)
//     → usar execute() ou query() — usa prepared statements
// ─────────────────────────────────────────────────────────────────────────────

const migrations: Array<{
  version: number;
  name: string;
  up: () => Promise<void>;
}> = [
  {
    version: 1,
    name: 'create_initial_tables',
    up: async () => {
      // Tabela de carteiras rastreadas
      await ddl(`
        CREATE TABLE IF NOT EXISTS wallets (
          address          VARCHAR(42)  NOT NULL,
          label            VARCHAR(100) DEFAULT NULL,
          first_seen       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
          last_seen        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
          total_transactions INT        NOT NULL DEFAULT 0,
          is_contract      TINYINT(1)   NOT NULL DEFAULT 0,
          created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (address)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      `);

      // Tabela de transações indexadas
      await ddl(`
        CREATE TABLE IF NOT EXISTS transactions (
          hash             VARCHAR(66)   NOT NULL,
          wallet_address   VARCHAR(42)   NOT NULL,
          block_number     BIGINT        NOT NULL,
          timestamp        DATETIME      NOT NULL,
          from_address     VARCHAR(42)   NOT NULL,
          to_address       VARCHAR(42)   DEFAULT NULL,
          value_eth        DECIMAL(36,18) NOT NULL DEFAULT 0,
          value_usd        DECIMAL(20,6)  DEFAULT NULL,
          token_address    VARCHAR(42)   DEFAULT NULL,
          token_symbol     VARCHAR(20)   DEFAULT NULL,
          token_decimals   INT           DEFAULT NULL,
          token_amount     DECIMAL(65,0) DEFAULT NULL,
          gas_used         BIGINT        DEFAULT NULL,
          gas_price        BIGINT        DEFAULT NULL,
          tx_type          VARCHAR(20)   NOT NULL DEFAULT 'other',
          is_win           TINYINT(1)    DEFAULT NULL,
          pnl              DECIMAL(20,6) DEFAULT NULL,
          created_at       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (hash),
          CONSTRAINT fk_tx_wallet FOREIGN KEY (wallet_address) REFERENCES wallets(address)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      `);

      // Tabela de eventos de swap
      await ddl(`
        CREATE TABLE IF NOT EXISTS swap_events (
          id                 BIGINT        NOT NULL AUTO_INCREMENT,
          tx_hash            VARCHAR(66)   NOT NULL,
          block_number       BIGINT        NOT NULL,
          timestamp          DATETIME      NOT NULL,
          wallet_address     VARCHAR(42)   NOT NULL,
          dex_address        VARCHAR(42)   NOT NULL,
          dex_name           VARCHAR(50)   DEFAULT NULL,
          token_in_address   VARCHAR(42)   NOT NULL,
          token_in_symbol    VARCHAR(20)   DEFAULT NULL,
          token_in_amount    DECIMAL(65,0) NOT NULL,
          token_out_address  VARCHAR(42)   NOT NULL,
          token_out_symbol   VARCHAR(20)   DEFAULT NULL,
          token_out_amount   DECIMAL(65,0) NOT NULL,
          value_usd          DECIMAL(20,6) DEFAULT NULL,
          pnl                DECIMAL(20,6) DEFAULT NULL,
          is_win             TINYINT(1)    DEFAULT NULL,
          created_at         DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uq_swap_tx_dex (tx_hash, dex_address),
          CONSTRAINT fk_swap_wallet FOREIGN KEY (wallet_address) REFERENCES wallets(address)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      `);

      // Tabela de informações de tokens
      await ddl(`
        CREATE TABLE IF NOT EXISTS tokens (
          address          VARCHAR(42)    NOT NULL,
          symbol           VARCHAR(20)    DEFAULT NULL,
          name             VARCHAR(100)   DEFAULT NULL,
          decimals         INT            NOT NULL DEFAULT 18,
          price_usd        DECIMAL(30,10) DEFAULT NULL,
          price_updated_at DATETIME       DEFAULT NULL,
          is_verified      TINYINT(1)     NOT NULL DEFAULT 0,
          created_at       DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at       DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (address)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      `);

      // Tabela de métricas dos KOLs
      await ddl(`
        CREATE TABLE IF NOT EXISTS kol_metrics (
          id                   BIGINT        NOT NULL AUTO_INCREMENT,
          wallet_address       VARCHAR(42)   NOT NULL,
          period               VARCHAR(20)   NOT NULL,
          period_start         DATETIME      NOT NULL,
          period_end           DATETIME      NOT NULL,
          wins                 INT           NOT NULL DEFAULT 0,
          losses               INT           NOT NULL DEFAULT 0,
          total_trades         INT           NOT NULL DEFAULT 0,
          profit_eth           DECIMAL(36,18) NOT NULL DEFAULT 0,
          profit_usd           DECIMAL(20,6)  NOT NULL DEFAULT 0,
          win_rate             DECIMAL(5,2)   NOT NULL DEFAULT 0,
          avg_trade_size_eth   DECIMAL(36,18) NOT NULL DEFAULT 0,
          best_trade_pnl       DECIMAL(20,6)  DEFAULT NULL,
          worst_trade_pnl      DECIMAL(20,6)  DEFAULT NULL,
          unique_tokens_traded INT            NOT NULL DEFAULT 0,
          last_updated         DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          created_at           DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uq_kol_period (wallet_address, period, period_start),
          CONSTRAINT fk_kol_wallet FOREIGN KEY (wallet_address) REFERENCES wallets(address)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      `);

      // Tabela de estado do indexador
      await ddl(`
        CREATE TABLE IF NOT EXISTS indexer_state (
          id                 INT           NOT NULL DEFAULT 1,
          last_indexed_block BIGINT        NOT NULL DEFAULT 0,
          last_updated       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          is_syncing         TINYINT(1)    NOT NULL DEFAULT 0,
          sync_progress      DECIMAL(5,2)  DEFAULT NULL,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      `);

      // Inserir estado inicial do indexador (ignora se já existir)
      await execute(
        'INSERT IGNORE INTO indexer_state (id, last_indexed_block) VALUES (?, ?)',
        [1, 0]
      );
    },
  },

  {
    version: 2,
    name: 'create_indexes',
    // MySQL não suporta CREATE INDEX IF NOT EXISTS.
    // Verificamos information_schema antes de cada CREATE INDEX.
    up: async () => {
      const indexDefs: Array<{ table: string; name: string; columns: string }> = [
        { table: 'transactions', name: 'idx_transactions_wallet',    columns: 'wallet_address' },
        { table: 'transactions', name: 'idx_transactions_timestamp', columns: 'timestamp' },
        { table: 'transactions', name: 'idx_transactions_block',     columns: 'block_number' },
        { table: 'transactions', name: 'idx_transactions_type',      columns: 'tx_type' },
        { table: 'swap_events',  name: 'idx_swap_events_wallet',     columns: 'wallet_address' },
        { table: 'swap_events',  name: 'idx_swap_events_timestamp',  columns: 'timestamp' },
        { table: 'swap_events',  name: 'idx_swap_events_token_in',   columns: 'token_in_address' },
        { table: 'swap_events',  name: 'idx_swap_events_token_out',  columns: 'token_out_address' },
        { table: 'kol_metrics',  name: 'idx_kol_metrics_wallet',     columns: 'wallet_address' },
        { table: 'kol_metrics',  name: 'idx_kol_metrics_period',     columns: 'period, period_start' },
        { table: 'kol_metrics',  name: 'idx_kol_metrics_profit',     columns: 'profit_usd' },
        { table: 'wallets',      name: 'idx_wallets_last_seen',      columns: 'last_seen' },
      ];

      for (const idx of indexDefs) {
        // Verificar se o índice já existe (query com prepared statement é seguro aqui)
        const exists = await query<{ cnt: number }>(
          `SELECT COUNT(*) AS cnt
           FROM information_schema.STATISTICS
           WHERE table_schema = DATABASE()
             AND table_name   = ?
             AND index_name   = ?`,
          [idx.table, idx.name]
        );

        if (!exists[0] || exists[0].cnt === 0) {
          // DDL não suporta prepared statements — usar ddl()
          await ddl(`CREATE INDEX ${idx.name} ON ${idx.table}(${idx.columns})`);
          logger.debug(`Created index ${idx.name} on ${idx.table}`);
        } else {
          logger.debug(`Index ${idx.name} already exists, skipping`);
        }
      }
    },
  },

  {
    version: 4,
    name: 'add_holding_time_and_follow_score',
    // Novas métricas para avaliar se uma carteira vale a pena seguir:
    //   holding_time_avg_s  — tempo médio de posição em segundos
    //   scalping_rate       — % de trades abaixo do limiar de scalping
    //   follow_score        — nota 0-100 composta por win rate + holding time
    up: async () => {
      // swap_events: registrar o tempo de holding de cada trade
      await ddl('ALTER TABLE swap_events ADD COLUMN IF NOT EXISTS holding_time_s INT DEFAULT NULL COMMENT \'Segundos entre compra e venda do token\'');

      // kol_metrics: métricas agregadas de holding e follow score
      await ddl('ALTER TABLE kol_metrics ADD COLUMN IF NOT EXISTS holding_time_avg_s   INT           DEFAULT NULL COMMENT \'Média de segundos nas posições\'');
      await ddl('ALTER TABLE kol_metrics ADD COLUMN IF NOT EXISTS holding_time_median_s INT          DEFAULT NULL COMMENT \'Mediana de segundos nas posições\'');
      await ddl('ALTER TABLE kol_metrics ADD COLUMN IF NOT EXISTS scalping_trades       INT           NOT NULL DEFAULT 0 COMMENT \'Trades abaixo do limiar de scalping\'');
      await ddl('ALTER TABLE kol_metrics ADD COLUMN IF NOT EXISTS scalping_rate         DECIMAL(5,2)  NOT NULL DEFAULT 0 COMMENT \'% de trades considerados scalping\'');
      await ddl('ALTER TABLE kol_metrics ADD COLUMN IF NOT EXISTS follow_score          DECIMAL(5,2)  NOT NULL DEFAULT 0 COMMENT \'Nota 0-100 para seguir o trader\'');
    },
  },

  {
    version: 3,
    name: 'widen_decimal_columns',
    // DECIMAL(20,6) suporta no máximo ~99 trilhões com 6 casas decimais.
    // Traders com grande volume acumulado ultrapassam esse limite.
    // Aumentamos para DECIMAL(36,6) em todas as colunas de valor monetário.
    up: async () => {
      // kol_metrics
      await ddl('ALTER TABLE kol_metrics MODIFY COLUMN profit_usd      DECIMAL(36,6) NOT NULL DEFAULT 0');
      await ddl('ALTER TABLE kol_metrics MODIFY COLUMN best_trade_pnl  DECIMAL(36,6) DEFAULT NULL');
      await ddl('ALTER TABLE kol_metrics MODIFY COLUMN worst_trade_pnl DECIMAL(36,6) DEFAULT NULL');
      // swap_events
      await ddl('ALTER TABLE swap_events MODIFY COLUMN value_usd DECIMAL(36,6) DEFAULT NULL');
      await ddl('ALTER TABLE swap_events MODIFY COLUMN pnl       DECIMAL(36,6) DEFAULT NULL');
      // transactions
      await ddl('ALTER TABLE transactions MODIFY COLUMN value_usd DECIMAL(36,6) DEFAULT NULL');
      await ddl('ALTER TABLE transactions MODIFY COLUMN pnl       DECIMAL(36,6) DEFAULT NULL');
    },
  },
];

export async function runMigrations(): Promise<void> {
  logger.info('Running database migrations...');

  // Garantir que a tabela de controle existe antes de tudo (DDL → ddl())
  await ddl(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version    INT          PRIMARY KEY,
      name       VARCHAR(100) NOT NULL,
      applied_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
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
      await migration.up();

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
