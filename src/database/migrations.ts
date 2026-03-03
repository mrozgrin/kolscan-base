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
    // Novas métricas para avaliar se uma carteira vale a pena seguir.
    // ALTER TABLE ADD COLUMN IF NOT EXISTS só existe no MySQL >= 8.0.3.
    // Para compatibilidade com versões anteriores, verificamos a existência
    // da coluna via information_schema.COLUMNS antes de cada ALTER TABLE.
    up: async () => {
      // Helper: adiciona coluna apenas se ela ainda não existir
      async function addColumnIfMissing(
        table: string,
        column: string,
        definition: string
      ): Promise<void> {
        const exists = await query<{ cnt: number }>(
          `SELECT COUNT(*) AS cnt
           FROM information_schema.COLUMNS
           WHERE table_schema = DATABASE()
             AND table_name   = ?
             AND column_name  = ?`,
          [table, column]
        );
        if (!exists[0] || exists[0].cnt === 0) {
          await ddl(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
        }
      }

      // swap_events
      await addColumnIfMissing('swap_events', 'holding_time_s',
        "INT DEFAULT NULL COMMENT 'Segundos entre compra e venda do token'");

      // kol_metrics
      await addColumnIfMissing('kol_metrics', 'holding_time_avg_s',
        "INT DEFAULT NULL COMMENT 'Media de segundos nas posicoes'");
      await addColumnIfMissing('kol_metrics', 'holding_time_median_s',
        "INT DEFAULT NULL COMMENT 'Mediana de segundos nas posicoes'");
      await addColumnIfMissing('kol_metrics', 'scalping_trades',
        "INT NOT NULL DEFAULT 0 COMMENT 'Trades abaixo do limiar de scalping'");
      await addColumnIfMissing('kol_metrics', 'scalping_rate',
        "DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '% de trades considerados scalping'");
      await addColumnIfMissing('kol_metrics', 'follow_score',
        "DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT 'Nota 0-100 para seguir o trader'");
    },
  },

  {
    version: 5,
    name: 'add_is_long_trade',
    // Nova lógica de holding:
    //   is_long_trade = 1 se holding_time_s >= SCALPING_THRESHOLD_SECONDS
    //   is_long_trade = 0 se holding_time_s <  SCALPING_THRESHOLD_SECONDS
    //   is_long_trade = NULL se holding_time_s é NULL (sem compra anterior registrada)
    //
    // A média de is_long_trade (AVG) resulta numa taxa 0.0-1.0 que representa
    // o percentual de trades com duração adequada. Isso evita que outliers
    // (ex: um trade de 30 dias) distorcem a média de holding time.
    up: async () => {
      async function addColumnIfMissing(
        table: string,
        column: string,
        definition: string
      ): Promise<void> {
        const exists = await query<{ cnt: number }>(
          `SELECT COUNT(*) AS cnt
           FROM information_schema.COLUMNS
           WHERE table_schema = DATABASE()
             AND table_name   = ?
             AND column_name  = ?`,
          [table, column]
        );
        if (!exists[0] || exists[0].cnt === 0) {
          await ddl(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
        }
      }

      await addColumnIfMissing('swap_events', 'is_long_trade',
        "TINYINT(1) DEFAULT NULL COMMENT '1 se holding >= threshold, 0 se < threshold, NULL se sem compra anterior'");

      // Índice para acelerar AVG(is_long_trade) nas queries de leaderboard
      const idxExists = await query<{ cnt: number }>(
        `SELECT COUNT(*) AS cnt
         FROM information_schema.STATISTICS
         WHERE table_schema = DATABASE()
           AND table_name   = 'swap_events'
           AND index_name   = 'idx_swap_events_is_long_trade'`
      );
      if (!idxExists[0] || idxExists[0].cnt === 0) {
        await ddl('CREATE INDEX idx_swap_events_is_long_trade ON swap_events(wallet_address, is_long_trade)');
      }
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
  {
    version: 6,
    name: 'add_disqualification_flags_and_score_components',
    up: async () => {
      // ── wallets: flags de desqualificação ──────────────────────────────────
      const walletCols = await query<{ COLUMN_NAME: string }>(
        `SELECT COLUMN_NAME FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wallets'`
      );
      const wCols = new Set(walletCols.map((r) => r.COLUMN_NAME));

      if (!wCols.has('flag_scalper'))
        await ddl(`ALTER TABLE wallets ADD COLUMN flag_scalper              TINYINT(1)    NOT NULL DEFAULT 0`);
      if (!wCols.has('flag_bundler'))
        await ddl(`ALTER TABLE wallets ADD COLUMN flag_bundler              TINYINT(1)    NOT NULL DEFAULT 0`);
      if (!wCols.has('flag_creator_funded'))
        await ddl(`ALTER TABLE wallets ADD COLUMN flag_creator_funded       TINYINT(1)    NOT NULL DEFAULT 0`);
      if (!wCols.has('flag_sybil'))
        await ddl(`ALTER TABLE wallets ADD COLUMN flag_sybil                TINYINT(1)    NOT NULL DEFAULT 0`);
      if (!wCols.has('scalper_copiability_index'))
        await ddl(`ALTER TABLE wallets ADD COLUMN scalper_copiability_index DECIMAL(5,4)  DEFAULT NULL`);
      if (!wCols.has('scalper_copiable_trades'))
        await ddl(`ALTER TABLE wallets ADD COLUMN scalper_copiable_trades   INT           DEFAULT NULL`);
      if (!wCols.has('scalper_total_trades'))
        await ddl(`ALTER TABLE wallets ADD COLUMN scalper_total_trades      INT           DEFAULT NULL`);
      if (!wCols.has('scalper_avg_holding_time'))
        await ddl(`ALTER TABLE wallets ADD COLUMN scalper_avg_holding_time  DECIMAL(10,2) DEFAULT NULL`);
      if (!wCols.has('scalper_checked_at'))
        await ddl(`ALTER TABLE wallets ADD COLUMN scalper_checked_at        DATETIME      DEFAULT NULL`);
      if (!wCols.has('bundler_same_block_pct'))
        await ddl(`ALTER TABLE wallets ADD COLUMN bundler_same_block_pct    DECIMAL(5,2)  DEFAULT NULL`);
      if (!wCols.has('bundler_same_block_count'))
        await ddl(`ALTER TABLE wallets ADD COLUMN bundler_same_block_count  INT           DEFAULT NULL`);
      if (!wCols.has('creator_funded_token'))
        await ddl(`ALTER TABLE wallets ADD COLUMN creator_funded_token      VARCHAR(42)   DEFAULT NULL`);
      if (!wCols.has('creator_funded_at'))
        await ddl(`ALTER TABLE wallets ADD COLUMN creator_funded_at         DATETIME      DEFAULT NULL`);
      if (!wCols.has('sybil_cluster_id'))
        await ddl(`ALTER TABLE wallets ADD COLUMN sybil_cluster_id          INT           DEFAULT NULL`);
      if (!wCols.has('is_disqualified'))
        await ddl(`ALTER TABLE wallets ADD COLUMN is_disqualified           TINYINT(1)    NOT NULL DEFAULT 0`);
      if (!wCols.has('flags_updated_at'))
        await ddl(`ALTER TABLE wallets ADD COLUMN flags_updated_at          DATETIME      DEFAULT NULL`);

      // Índices em wallets
      const wIdxs = await query<{ INDEX_NAME: string }>(
        `SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wallets'`
      );
      const wIdx = new Set(wIdxs.map((r) => r.INDEX_NAME));
      if (!wIdx.has('idx_wallets_disqualified'))
        await ddl(`CREATE INDEX idx_wallets_disqualified ON wallets(is_disqualified)`);
      if (!wIdx.has('idx_wallets_scalper'))
        await ddl(`CREATE INDEX idx_wallets_scalper ON wallets(flag_scalper)`);
      if (!wIdx.has('idx_wallets_bundler'))
        await ddl(`CREATE INDEX idx_wallets_bundler ON wallets(flag_bundler)`);
      if (!wIdx.has('idx_wallets_copiability'))
        await ddl(`CREATE INDEX idx_wallets_copiability ON wallets(scalper_copiability_index)`);

      // ── kol_metrics: componentes do score ────────────────────────────────
      const kmCols = await query<{ COLUMN_NAME: string }>(
        `SELECT COLUMN_NAME FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kol_metrics'`
      );
      const kmC = new Set(kmCols.map((r) => r.COLUMN_NAME));

      const kmNewCols: Record<string, string> = {
        long_trade_rate_pct:         'DECIMAL(5,2) DEFAULT NULL',
        profit_pct:                  'DECIMAL(10,4) DEFAULT NULL',
        followability_hold_score:    'DECIMAL(5,2) DEFAULT NULL',
        followability_volume_score:  'DECIMAL(5,2) DEFAULT NULL',
        followability_liq_score:     'DECIMAL(5,2) DEFAULT NULL',
        followability_final:         'DECIMAL(5,2) DEFAULT NULL',
        consistency_wr_stability:    'DECIMAL(5,2) DEFAULT NULL',
        consistency_pnl_stability:   'DECIMAL(5,2) DEFAULT NULL',
        consistency_diversification: 'DECIMAL(5,2) DEFAULT NULL',
        consistency_final:           'DECIMAL(5,2) DEFAULT NULL',
        pnl_relative_score:          'DECIMAL(5,2) DEFAULT NULL',
        pnl_profit_factor:           'DECIMAL(10,2) DEFAULT NULL',
        pnl_pf_score:                'DECIMAL(5,2) DEFAULT NULL',
        pnl_final:                   'DECIMAL(5,2) DEFAULT NULL',
        win_rate_score:              'DECIMAL(5,2) DEFAULT NULL',
        trades_per_day:              'DECIMAL(8,2) DEFAULT NULL',
        avg_position_size_usd:       'DECIMAL(36,6) DEFAULT NULL',
        gross_profit_usd:            'DECIMAL(36,6) DEFAULT NULL',
        gross_loss_usd:              'DECIMAL(36,6) DEFAULT NULL',
        monthly_wr_cv:               'DECIMAL(5,2) DEFAULT NULL',
        profitable_months_ratio:     'DECIMAL(5,2) DEFAULT NULL',
        diversification_ratio:       'DECIMAL(5,4) DEFAULT NULL',
        p90_benchmark_usd:           'DECIMAL(36,6) DEFAULT NULL',
        has_sufficient_data:         'TINYINT(1) NOT NULL DEFAULT 1',
      };
      for (const [col, def] of Object.entries(kmNewCols)) {
        if (!kmC.has(col))
          await ddl(`ALTER TABLE kol_metrics ADD COLUMN ${col} ${def}`);
      }

      // Índice composto no leaderboard
      const kmIdxs = await query<{ INDEX_NAME: string }>(
        `SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kol_metrics'`
      );
      const kmIdx = new Set(kmIdxs.map((r) => r.INDEX_NAME));
      if (!kmIdx.has('idx_kol_follow_score'))
        await ddl(`CREATE INDEX idx_kol_follow_score ON kol_metrics(follow_score, period, period_start)`);

      // ── tokens: dados de deploy ───────────────────────────────────────────
      const tkCols = await query<{ COLUMN_NAME: string }>(
        `SELECT COLUMN_NAME FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tokens'`
      );
      const tkC = new Set(tkCols.map((r) => r.COLUMN_NAME));
      if (!tkC.has('deployer_address'))
        await ddl(`ALTER TABLE tokens ADD COLUMN deployer_address VARCHAR(42) DEFAULT NULL`);
      if (!tkC.has('deploy_tx_hash'))
        await ddl(`ALTER TABLE tokens ADD COLUMN deploy_tx_hash   VARCHAR(66) DEFAULT NULL`);
      if (!tkC.has('deploy_block'))
        await ddl(`ALTER TABLE tokens ADD COLUMN deploy_block      BIGINT      DEFAULT NULL`);
      if (!tkC.has('deploy_timestamp'))
        await ddl(`ALTER TABLE tokens ADD COLUMN deploy_timestamp  DATETIME    DEFAULT NULL`);

      const tkIdxs = await query<{ INDEX_NAME: string }>(
        `SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tokens'`
      );
      const tkIdx = new Set(tkIdxs.map((r) => r.INDEX_NAME));
      if (!tkIdx.has('idx_tokens_deployer'))
        await ddl(`CREATE INDEX idx_tokens_deployer ON tokens(deployer_address)`);

      // ── sybil_clusters ────────────────────────────────────────────────────
      await ddl(`
        CREATE TABLE IF NOT EXISTS sybil_clusters (
          id               INT          NOT NULL AUTO_INCREMENT,
          detected_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
          wallet_count     INT          NOT NULL,
          common_funder    VARCHAR(42)  DEFAULT NULL,
          sync_trade_count INT          DEFAULT NULL,
          jaccard_avg      DECIMAL(5,4) DEFAULT NULL,
          notes            TEXT         DEFAULT NULL,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      `);

      // ── sybil_cluster_members ─────────────────────────────────────────────
      await ddl(`
        CREATE TABLE IF NOT EXISTS sybil_cluster_members (
          cluster_id       INT         NOT NULL,
          wallet_address   VARCHAR(42) NOT NULL,
          added_at         DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
          signals_matched  JSON        DEFAULT NULL,
          PRIMARY KEY (cluster_id, wallet_address),
          CONSTRAINT fk_scm_cluster FOREIGN KEY (cluster_id) REFERENCES sybil_clusters(id),
          CONSTRAINT fk_scm_wallet  FOREIGN KEY (wallet_address) REFERENCES wallets(address)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      `);

      // ── kol_score_history ─────────────────────────────────────────────────
      await ddl(`
        CREATE TABLE IF NOT EXISTS kol_score_history (
          id               BIGINT      NOT NULL AUTO_INCREMENT,
          wallet_address   VARCHAR(42) NOT NULL,
          calculated_at    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
          period           VARCHAR(20) NOT NULL,
          follow_score     DECIMAL(5,2) NOT NULL,
          followability    DECIMAL(5,2) NOT NULL DEFAULT 0,
          consistency      DECIMAL(5,2) NOT NULL DEFAULT 0,
          pnl_score        DECIMAL(5,2) NOT NULL DEFAULT 0,
          win_rate_score   DECIMAL(5,2) NOT NULL DEFAULT 0,
          total_trades     INT          NOT NULL DEFAULT 0,
          PRIMARY KEY (id),
          KEY idx_ksh_wallet_date (wallet_address, calculated_at),
          CONSTRAINT fk_ksh_wallet FOREIGN KEY (wallet_address) REFERENCES wallets(address)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      `);
    },
  },

  // ───────────────────────────────────────────────────────────────────────────
  // Migration 7: garantir colunas críticas em kol_metrics, wallets e tokens
  // Corrige casos onde a migration 6 foi registrada mas falhou parcialmente,
  // deixando colunas faltando no banco.
  // ───────────────────────────────────────────────────────────────────────────
  {
    version: 7,
    name: 'ensure_critical_columns',
    async up() {
      // ── kol_metrics: colunas obrigatórias para o metrics-service ──────────
      const kmCols = await query<{ COLUMN_NAME: string }>(
        `SELECT COLUMN_NAME FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kol_metrics'`
      );
      const kmC = new Set(kmCols.map((r) => r.COLUMN_NAME));

      const criticalKmCols: Record<string, string> = {
        long_trade_rate_pct:         'DECIMAL(5,2) DEFAULT NULL',
        profit_pct:                  'DECIMAL(10,4) DEFAULT NULL',
        scalping_rate:               'DECIMAL(5,2) DEFAULT NULL',
        holding_time_avg_s:          'INT DEFAULT NULL',
        follow_score:                'DECIMAL(5,2) DEFAULT NULL',
        followability_hold_score:    'DECIMAL(5,2) DEFAULT NULL',
        followability_volume_score:  'DECIMAL(5,2) DEFAULT NULL',
        followability_liq_score:     'DECIMAL(5,2) DEFAULT NULL',
        followability_final:         'DECIMAL(5,2) DEFAULT NULL',
        consistency_wr_stability:    'DECIMAL(5,2) DEFAULT NULL',
        consistency_pnl_stability:   'DECIMAL(5,2) DEFAULT NULL',
        consistency_diversification: 'DECIMAL(5,2) DEFAULT NULL',
        consistency_final:           'DECIMAL(5,2) DEFAULT NULL',
        pnl_relative_score:          'DECIMAL(5,2) DEFAULT NULL',
        pnl_profit_factor:           'DECIMAL(10,2) DEFAULT NULL',
        pnl_pf_score:                'DECIMAL(5,2) DEFAULT NULL',
        pnl_final:                   'DECIMAL(5,2) DEFAULT NULL',
        win_rate_score:              'DECIMAL(5,2) DEFAULT NULL',
        trades_per_day:              'DECIMAL(8,2) DEFAULT NULL',
        avg_position_size_usd:       'DECIMAL(36,6) DEFAULT NULL',
        gross_profit_usd:            'DECIMAL(36,6) DEFAULT NULL',
        gross_loss_usd:              'DECIMAL(36,6) DEFAULT NULL',
        monthly_wr_cv:               'DECIMAL(5,2) DEFAULT NULL',
        profitable_months_ratio:     'DECIMAL(5,2) DEFAULT NULL',
        diversification_ratio:       'DECIMAL(5,4) DEFAULT NULL',
        p90_benchmark_usd:           'DECIMAL(36,6) DEFAULT NULL',
        has_sufficient_data:         'TINYINT(1) NOT NULL DEFAULT 1',
      };

      for (const [col, def] of Object.entries(criticalKmCols)) {
        if (!kmC.has(col)) {
          await ddl(`ALTER TABLE kol_metrics ADD COLUMN ${col} ${def}`);
          logger.debug(`Added column kol_metrics.${col}`);
        }
      }

      // ── wallets: colunas de flags e desqualificação ───────────────────────
      const wCols = await query<{ COLUMN_NAME: string }>(
        `SELECT COLUMN_NAME FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'wallets'`
      );
      const wC = new Set(wCols.map((r) => r.COLUMN_NAME));

      const criticalWCols: Record<string, string> = {
        flag_scalper:               'TINYINT(1) NOT NULL DEFAULT 0',
        flag_bundler:               'TINYINT(1) NOT NULL DEFAULT 0',
        flag_creator_funded:        'TINYINT(1) NOT NULL DEFAULT 0',
        flag_sybil:                 'TINYINT(1) NOT NULL DEFAULT 0',
        scalper_copiability_index:  'DECIMAL(5,4) DEFAULT NULL',
        scalper_avg_hold_s:         'INT DEFAULT NULL',
        scalper_trade_count:        'INT DEFAULT NULL',
        bundler_same_block_count:   'INT DEFAULT NULL',
        creator_funded_token:       'VARCHAR(42) DEFAULT NULL',
        creator_funded_at:          'DATETIME DEFAULT NULL',
        sybil_cluster_id:           'INT DEFAULT NULL',
        is_disqualified:            'TINYINT(1) NOT NULL DEFAULT 0',
        disqualification_reason:    'VARCHAR(30) DEFAULT NULL',
        flags_updated_at:           'DATETIME DEFAULT NULL',
      };

      for (const [col, def] of Object.entries(criticalWCols)) {
        if (!wC.has(col)) {
          await ddl(`ALTER TABLE wallets ADD COLUMN ${col} ${def}`);
          logger.debug(`Added column wallets.${col}`);
        }
      }

      // ── tokens: colunas de deploy e market data ───────────────────────────
      const tkCols = await query<{ COLUMN_NAME: string }>(
        `SELECT COLUMN_NAME FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tokens'`
      );
      const tkC = new Set(tkCols.map((r) => r.COLUMN_NAME));

      const criticalTkCols: Record<string, string> = {
        name:             'VARCHAR(100) DEFAULT NULL',
        deployer_address: 'VARCHAR(42) DEFAULT NULL',
        deploy_tx_hash:   'VARCHAR(66) DEFAULT NULL',
        deploy_block:     'BIGINT DEFAULT NULL',
        deploy_timestamp: 'DATETIME DEFAULT NULL',
      };

      for (const [col, def] of Object.entries(criticalTkCols)) {
        if (!tkC.has(col)) {
          await ddl(`ALTER TABLE tokens ADD COLUMN ${col} ${def}`);
          logger.debug(`Added column tokens.${col}`);
        }
      }

      // ── Índices críticos ──────────────────────────────────────────────────
      const allIdxs = await query<{ TABLE_NAME: string; INDEX_NAME: string }>(
        `SELECT TABLE_NAME, INDEX_NAME FROM information_schema.STATISTICS
         WHERE TABLE_SCHEMA = DATABASE()
           AND TABLE_NAME IN ('wallets', 'kol_metrics', 'swap_events')`
      );
      const idxSet = new Set(allIdxs.map((r) => `${r.TABLE_NAME}.${r.INDEX_NAME}`));

      if (!idxSet.has('wallets.idx_wallets_disqualified'))
        await ddl(`CREATE INDEX idx_wallets_disqualified ON wallets(is_disqualified)`);
      if (!idxSet.has('kol_metrics.idx_kol_follow_score'))
        await ddl(`CREATE INDEX idx_kol_follow_score ON kol_metrics(follow_score, period, period_start)`);
      if (!idxSet.has('swap_events.idx_se_is_long_trade'))
        await ddl(`CREATE INDEX idx_se_is_long_trade ON swap_events(is_long_trade)`);
    },
  },

  // ───────────────────────────────────────────────────────────────────────────
  // Migration 8: aumentar precisão de profit_pct e pnl_profit_factor
  // profit_pct pode ultrapassar 999999% em memecoins (ex: +10.000.000%)
  // pnl_profit_factor pode ultrapassar 99.999.999 quando gross_loss ≈ 0
  // ───────────────────────────────────────────────────────────────────────────
  {
    version: 8,
    name: 'fix_decimal_overflow_profit_pct_and_profit_factor',
    async up() {
      // profit_pct: DECIMAL(10,4) → DECIMAL(20,4)  (suporta até 9.999.999.999.999.999,9999)
      await ddl(`ALTER TABLE kol_metrics MODIFY COLUMN profit_pct DECIMAL(20,4) DEFAULT NULL`);

      // pnl_profit_factor: DECIMAL(10,2) → DECIMAL(12,2)  (suporta até 9.999.999.999,99)
      // O código já faz cap em 9999, mas aumentamos a coluna por segurança
      await ddl(`ALTER TABLE kol_metrics MODIFY COLUMN pnl_profit_factor DECIMAL(12,2) DEFAULT NULL`);
    },
  },

  // ───────────────────────────────────────────────────────────────────────────
  // Migration 9: modelo de posição — PnL real calculado entre compra e venda
  //
  // Conceito:
  //   Cada vez que a wallet compra um token (ex: WETH → XYZCOIN), abre ou
  //   aumenta uma posição. O custo de entrada é acumulado em "custo médio
  //   ponderado" (VWAP) na moeda base (ex: WETH).
  //
  //   Quando a wallet vende (XYZCOIN → WETH), o PnL é calculado como:
  //     pnl_base = qty_vendida × avg_cost_base_per_token
  //     pnl_base = valor_recebido_base - custo_proporcional_base
  //
  //   O resultado é registrado na moeda base (ex: WETH), não em USD.
  //   O valor em USD é registrado adicionalmente para fins de leaderboard.
  //
  // Tabela positions:
  //   Rastreia posições abertas por (wallet, token).
  //   Atualizada a cada compra (acumula) e a cada venda (reduz).
  //
  // Colunas adicionadas em swap_events:
  //   pnl_base        — PnL na moeda base (null em compras, valor real em vendas)
  //   pnl_base_token  — Endereço da moeda base em que o PnL foi registrado
  //   pnl_base_symbol — Símbolo da moeda base (ex: WETH, USDC)
  //   swap_type       — 'buy' | 'sell' | 'swap' (troca entre dois não-base)
  // ───────────────────────────────────────────────────────────────────────────
  {
    version: 9,
    name: 'add_positions_and_base_pnl',
    async up() {
      // ── Tabela positions ──────────────────────────────────────────────────
      await ddl(`
        CREATE TABLE IF NOT EXISTS positions (
          wallet_address      VARCHAR(42)    NOT NULL,
          token_address       VARCHAR(42)    NOT NULL,
          base_token_address  VARCHAR(42)    NOT NULL COMMENT 'Moeda base da posição (ex: WETH)',
          base_token_symbol   VARCHAR(20)    NOT NULL DEFAULT '' COMMENT 'Símbolo da moeda base',
          qty_open            DECIMAL(65,18) NOT NULL DEFAULT 0 COMMENT 'Quantidade do token ainda em carteira',
          cost_basis_base     DECIMAL(65,18) NOT NULL DEFAULT 0 COMMENT 'Custo total de entrada na moeda base',
          avg_cost_base       DECIMAL(65,18) NOT NULL DEFAULT 0 COMMENT 'Custo médio por unidade do token na moeda base',
          opened_at           DATETIME       NOT NULL,
          last_updated        DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (wallet_address, token_address),
          CONSTRAINT fk_pos_wallet FOREIGN KEY (wallet_address) REFERENCES wallets(address)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      `);

      // ── Índices em positions ──────────────────────────────────────────────
      const posIdxs = await query<{ INDEX_NAME: string }>(
        `SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'positions'`
      );
      const posIdx = new Set(posIdxs.map((r) => r.INDEX_NAME));
      if (!posIdx.has('idx_positions_wallet'))
        await ddl(`CREATE INDEX idx_positions_wallet ON positions(wallet_address)`);
      if (!posIdx.has('idx_positions_token'))
        await ddl(`CREATE INDEX idx_positions_token ON positions(token_address)`);

      // ── Novas colunas em swap_events ──────────────────────────────────────
      const seCols = await query<{ COLUMN_NAME: string }>(
        `SELECT COLUMN_NAME FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'swap_events'`
      );
      const seC = new Set(seCols.map((r) => r.COLUMN_NAME));

      if (!seC.has('pnl_base'))
        await ddl(`ALTER TABLE swap_events ADD COLUMN pnl_base DECIMAL(65,18) DEFAULT NULL
          COMMENT 'PnL na moeda base (null em compras, valor real em vendas)'`);
      if (!seC.has('pnl_base_token'))
        await ddl(`ALTER TABLE swap_events ADD COLUMN pnl_base_token VARCHAR(42) DEFAULT NULL
          COMMENT 'Endereço da moeda base em que o PnL foi registrado'`);
      if (!seC.has('pnl_base_symbol'))
        await ddl(`ALTER TABLE swap_events ADD COLUMN pnl_base_symbol VARCHAR(20) DEFAULT NULL
          COMMENT 'Símbolo da moeda base (ex: WETH, USDC)'`);
      if (!seC.has('swap_type'))
        await ddl(`ALTER TABLE swap_events ADD COLUMN swap_type VARCHAR(10) DEFAULT NULL
          COMMENT 'buy | sell | swap'`);

      // ── Índice para acelerar queries de PnL por período ───────────────────
      const seIdxs = await query<{ INDEX_NAME: string }>(
        `SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'swap_events'`
      );
      const seIdx = new Set(seIdxs.map((r) => r.INDEX_NAME));
      if (!seIdx.has('idx_se_swap_type'))
        await ddl(`CREATE INDEX idx_se_swap_type ON swap_events(wallet_address, swap_type, timestamp)`);
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
