/**
 * top-traders.ts
 *
 * Exibe os top traders no terminal com todas as variáveis calculadas.
 *
 * Uso:
 *   npm run top-traders                  → top 100, período 30 dias
 *   npm run top-traders -- --limit 50    → top 50
 *   npm run top-traders -- --period 7    → últimos 7 dias
 *   npm run top-traders -- --limit 10 --period 90
 */

import { query } from '../database/connection';
import { logger } from '../utils/logger';

// ─── Argumentos de linha de comando ──────────────────────────────────────────
const args = process.argv.slice(2);
function getArg(name: string, defaultVal: number): number {
  const idx = args.indexOf(`--${name}`);
  if (idx !== -1 && args[idx + 1]) return parseInt(args[idx + 1], 10) || defaultVal;
  return defaultVal;
}
const LIMIT       = getArg('limit', 100);
const PERIOD_DAYS = getArg('period', 30);

// ─── Cores ANSI ───────────────────────────────────────────────────────────────
const C = {
  reset:   '\x1b[0m',
  bold:    '\x1b[1m',
  dim:     '\x1b[2m',
  green:   '\x1b[32m',
  red:     '\x1b[31m',
  yellow:  '\x1b[33m',
  cyan:    '\x1b[36m',
  blue:    '\x1b[34m',
  magenta: '\x1b[35m',
  white:   '\x1b[97m',
  bgBlue:  '\x1b[44m',
  bgGreen: '\x1b[42m',
  bgRed:   '\x1b[41m',
};

function scoreColor(score: number): string {
  if (score >= 80) return C.green + C.bold;
  if (score >= 60) return C.cyan;
  if (score >= 40) return C.yellow;
  return C.red;
}

function pnlColor(val: number): string {
  return val >= 0 ? C.green : C.red;
}

function scoreBar(score: number, width = 20): string {
  const filled = Math.round((score / 100) * width);
  const bar = '█'.repeat(filled) + '░'.repeat(width - filled);
  return scoreColor(score) + bar + C.reset;
}

function scoreLabel(score: number): string {
  if (score >= 80) return `${C.green}${C.bold}EXCELENTE${C.reset}`;
  if (score >= 70) return `${C.cyan}MUITO BOM${C.reset}`;
  if (score >= 60) return `${C.cyan}BOM      ${C.reset}`;
  if (score >= 50) return `${C.yellow}ACEITÁVEL${C.reset}`;
  if (score >= 40) return `${C.yellow}FRACO    ${C.reset}`;
  return `${C.red}NÃO REC. ${C.reset}`;
}

function fmt(val: number | null | undefined, decimals = 2, suffix = ''): string {
  if (val === null || val === undefined) return `${C.dim}N/A${C.reset}`;
  return `${val.toFixed(decimals)}${suffix}`;
}

function fmtPnl(val: number | null | undefined, suffix = '%'): string {
  if (val === null || val === undefined) return `${C.dim}N/A${C.reset}`;
  const sign = val >= 0 ? '+' : '';
  return `${pnlColor(val)}${sign}${val.toFixed(2)}${suffix}${C.reset}`;
}

function fmtUsd(val: number | null | undefined): string {
  if (val === null || val === undefined) return `${C.dim}N/A${C.reset}`;
  const sign = val >= 0 ? '+' : '';
  const abs = Math.abs(val);
  let str: string;
  if (abs >= 1_000_000) str = `${(val / 1_000_000).toFixed(2)}M`;
  else if (abs >= 1_000) str = `${(val / 1_000).toFixed(2)}K`;
  else str = val.toFixed(2);
  return `${pnlColor(val)}${sign}$${str}${C.reset}`;
}

function fmtHold(seconds: number | null | undefined): string {
  if (!seconds) return `${C.dim}N/A${C.reset}`;
  const s = Math.round(Number(seconds));
  if (s < 60)    return `${s}s`;
  if (s < 3600)  return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
}

// Padding que ignora os escape codes ANSI ao calcular largura
function padEnd(str: string, len: number): string {
  const clean = str.replace(/\x1b\[[0-9;]*m/g, '');
  const pad = Math.max(0, len - clean.length);
  return str + ' '.repeat(pad);
}

function separator(char = '─', width = 130): string {
  return C.dim + char.repeat(width) + C.reset;
}

// ─── Tipos ────────────────────────────────────────────────────────────────────
interface TraderRow {
  wallet_address:              string;
  follow_score:                number;
  profit_usd:                  number;
  profit_pct:                  number;
  win_rate:                    number;
  total_trades:                number;
  wins:                        number;
  losses:                      number;
  trades_per_day:              number;
  holding_time_avg_s:          number;
  scalping_rate:               number;
  long_trade_rate_pct:         number;
  followability_final:         number;
  followability_hold_score:    number;
  followability_volume_score:  number;
  followability_liq_score:     number;
  consistency_final:           number;
  consistency_wr_stability:    number;
  consistency_pnl_stability:   number;
  consistency_diversification: number;
  monthly_wr_cv:               number;
  profitable_months_ratio:     number;
  diversification_ratio:       number;
  pnl_final:                   number;
  pnl_relative_score:          number;
  pnl_profit_factor:           number;
  pnl_pf_score:                number;
  gross_profit_usd:            number;
  gross_loss_usd:              number;
  p90_benchmark_usd:           number;
  win_rate_score:              number;
  unique_tokens_traded:        number;
  best_trade_pnl:              number;
  worst_trade_pnl:             number;
  has_sufficient_data:         number;
  label:                       string;
  period:                      string;
  period_start:                string;
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  const W = 130;
  console.log('\n' + separator('═', W));
  console.log(
    `${C.bgBlue}${C.white}${C.bold}  🏆  TOP TRADERS — KOLSCAN${' '.repeat(W - 28)}${C.reset}`
  );
  console.log(
    `${C.dim}  Período: últimos ${PERIOD_DAYS} dias  |  Exibindo top ${LIMIT} traders  |  ${new Date().toLocaleString('pt-BR')}${C.reset}`
  );
  console.log(
    `${C.dim}  Uso: npm run top-traders -- --limit 50 --period 7${C.reset}`
  );
  console.log(separator('═', W));

  // Mapear dias para o label usado na tabela kol_metrics
  const period = PERIOD_DAYS <= 1 ? 'daily' : PERIOD_DAYS <= 7 ? 'weekly' : PERIOD_DAYS <= 30 ? 'monthly' : 'all_time';

  const rows = await query<TraderRow>(
    `SELECT
       km.wallet_address,
       km.follow_score,
       km.profit_usd,
       km.profit_pct,
       km.win_rate,
       km.total_trades,
       km.wins,
       km.losses,
       km.trades_per_day,
       km.holding_time_avg_s,
       km.scalping_rate,
       km.long_trade_rate_pct,
       km.followability_final,
       km.followability_hold_score,
       km.followability_volume_score,
       km.followability_liq_score,
       km.consistency_final,
       km.consistency_wr_stability,
       km.consistency_pnl_stability,
       km.consistency_diversification,
       km.monthly_wr_cv,
       km.profitable_months_ratio,
       km.diversification_ratio,
       km.pnl_final,
       km.pnl_relative_score,
       km.pnl_profit_factor,
       km.pnl_pf_score,
       km.gross_profit_usd,
       km.gross_loss_usd,
       km.p90_benchmark_usd,
       km.win_rate_score,
       km.unique_tokens_traded,
       km.best_trade_pnl,
       km.worst_trade_pnl,
       km.has_sufficient_data,
       w.label,
       km.period,
       DATE_FORMAT(km.period_start, '%Y-%m-%d') AS period_start
     FROM kol_metrics km
     JOIN wallets w ON w.address = km.wallet_address
     WHERE km.period = ?
       AND km.follow_score IS NOT NULL
       AND w.is_disqualified = 0
     ORDER BY km.follow_score DESC
     LIMIT ${LIMIT}`,
    [period]
  );

  if (rows.length === 0) {
    console.log(`\n${C.yellow}  Nenhum trader encontrado para o período "${period}" (--period ${PERIOD_DAYS}).${C.reset}`);
    console.log(`  Possíveis causas:`);
    console.log(`    1. O analyzer ainda não rodou para este período.`);
    console.log(`    2. Tente outro período: --period 1, --period 7, --period 30, --period 90`);
    console.log(`    3. Verifique: SELECT DISTINCT period FROM kol_metrics LIMIT 10;\n`);
    process.exit(0);
  }

  const periodMap: Record<string, number> = { 'daily': 1, 'weekly': 7, 'monthly': 30, 'all_time': 365 };
  const days = periodMap[period] || PERIOD_DAYS;

  rows.forEach((t, idx) => {
    const rank  = idx + 1;
    const wallet = t.wallet_address;
    const name  = t.label ? `${C.bold}${t.label}${C.reset}` : wallet;
    const pnlDailyPct = t.profit_pct != null ? t.profit_pct / days : null;

    const rankStr = rank <= 3
      ? ['🥇', '🥈', '🥉'][rank - 1]
      : `${C.dim}#${rank.toString().padStart(3)}${C.reset}`;

    // ── Cabeçalho do trader ──────────────────────────────────────────────────
    console.log('\n' + separator('─', W));
    console.log(`  ${rankStr}  ${C.bold}${C.white}${name}${C.reset}`);
    if (t.label) {
      console.log(`       ${C.dim}${wallet}${C.reset}`);
    }
    console.log(
      `       ${scoreLabel(t.follow_score)}  ` +
      `${scoreBar(t.follow_score, 25)}  ` +
      `${scoreColor(t.follow_score)}${C.bold}${fmt(t.follow_score)}/100${C.reset}`
    );

    // ── Linha 1: PnL ────────────────────────────────────────────────────────
    console.log(
      `\n  ${C.bold}PnL Total:${C.reset} ${padEnd(fmtUsd(t.profit_usd), 14)}` +
      `  ${C.bold}PnL Médio/trade:${C.reset} ${padEnd(fmtPnl(t.profit_pct), 12)}` +
      `  ${C.bold}PnL Diário:${C.reset} ${padEnd(fmtPnl(pnlDailyPct), 12)}` +
      `  ${C.bold}Win Rate:${C.reset} ${fmtPnl(t.win_rate, '%')}`
    );

    // ── Linha 2: Trades ──────────────────────────────────────────────────────
    console.log(
      `  ${C.bold}Trades:${C.reset} ${String(t.total_trades).padEnd(4)}` +
      `  ${C.green}✓ Wins: ${t.wins}${C.reset}  ${C.red}✗ Losses: ${t.losses}${C.reset}` +
      `  ${C.bold}Trades/dia:${C.reset} ${padEnd(fmt(t.trades_per_day), 8)}` +
      `  ${C.bold}Tokens únicos:${C.reset} ${String(t.unique_tokens_traded ?? 'N/A').padEnd(4)}` +
      `  ${C.bold}Diversif.:${C.reset} ${fmt(t.diversification_ratio ? t.diversification_ratio * 100 : null, 1, '%')}`
    );

    // ── Linha 3: Tempo e Scalping ────────────────────────────────────────────
    console.log(
      `  ${C.bold}Hold médio:${C.reset} ${padEnd(fmtHold(t.holding_time_avg_s), 12)}` +
      `  ${C.bold}Scalping:${C.reset} ${padEnd(fmt(t.scalping_rate, 1, '%'), 8)}` +
      `  ${C.bold}Long trades:${C.reset} ${padEnd(fmt(t.long_trade_rate_pct, 1, '%'), 8)}` +
      `  ${C.bold}Melhor trade:${C.reset} ${padEnd(fmtPnl(t.best_trade_pnl), 12)}` +
      `  ${C.bold}Pior trade:${C.reset} ${fmtPnl(t.worst_trade_pnl)}`
    );

    // ── Componentes do Follow Score ──────────────────────────────────────────
    console.log(`\n  ${C.bold}${C.cyan}── Componentes do Follow Score ──────────────────────────────────────────────${C.reset}`);

    // Followability
    console.log(
      `  ${C.magenta}Followability ${C.reset}${scoreBar(t.followability_final, 18)} ` +
      `${padEnd(fmt(t.followability_final) + '/100', 10)}  ` +
      `${C.dim}Hold: ${fmt(t.followability_hold_score, 1).padEnd(6)} | Volume: ${fmt(t.followability_volume_score, 1).padEnd(6)} | Liq: ${fmt(t.followability_liq_score, 1)}${C.reset}`
    );

    // Consistência
    console.log(
      `  ${C.blue}Consistência  ${C.reset}${scoreBar(t.consistency_final, 18)} ` +
      `${padEnd(fmt(t.consistency_final) + '/100', 10)}  ` +
      `${C.dim}WR Stab: ${fmt(t.consistency_wr_stability, 1).padEnd(6)} | PnL Stab: ${fmt(t.consistency_pnl_stability, 1).padEnd(6)} | Divers: ${fmt(t.consistency_diversification, 1)}${C.reset}`
    );
    console.log(
      `  ${C.dim}                                                    ` +
      `WR CV: ${fmt(t.monthly_wr_cv, 1, '%').padEnd(10)} | Meses lucrativos: ${fmt(t.profitable_months_ratio, 1, '%')}${C.reset}`
    );

    // PnL Score
    console.log(
      `  ${C.green}PnL Score     ${C.reset}${scoreBar(t.pnl_final, 18)} ` +
      `${padEnd(fmt(t.pnl_final) + '/100', 10)}  ` +
      `${C.dim}Rel: ${fmt(t.pnl_relative_score, 1).padEnd(6)} | PF Score: ${fmt(t.pnl_pf_score, 1)}${C.reset}`
    );
    console.log(
      `  ${C.dim}                                                    ` +
      `Profit Factor: ${fmt(t.pnl_profit_factor, 2).padEnd(8)} | Gross Profit: ${fmtUsd(t.gross_profit_usd)} | Gross Loss: ${fmtUsd(t.gross_loss_usd)} | P90: ${fmtUsd(t.p90_benchmark_usd)}${C.reset}`
    );

    // Win Rate Score
    console.log(
      `  ${C.yellow}Win Rate      ${C.reset}${scoreBar(t.win_rate_score, 18)} ` +
      `${padEnd(fmt(t.win_rate_score) + '/100', 10)}  ` +
      `${C.dim}Win Rate: ${fmt(t.win_rate, 1, '%')}${C.reset}`
    );
  });

  // ── Tabela resumo ──────────────────────────────────────────────────────────
  console.log('\n' + separator('═', W));
  console.log(`${C.bold}${C.white}  RESUMO RÁPIDO — TOP ${rows.length} TRADERS${C.reset}`);
  console.log(separator('─', W));

  // Cabeçalho da tabela
  const H = {
    rank:   4,
    wallet: 44,
    score:  8,
    pnlUsd: 12,
    pnlPct: 10,
    pnlDay: 10,
    wr:     8,
    trades: 7,
    hold:   11,
    scalp:  7,
  };
  console.log(
    `  ${'#'.padEnd(H.rank)}  ` +
    `${'Wallet'.padEnd(H.wallet)}  ` +
    `${'Score'.padEnd(H.score)}  ` +
    `${'PnL USD'.padEnd(H.pnlUsd)}  ` +
    `${'PnL%/trade'.padEnd(H.pnlPct)}  ` +
    `${'PnL%/dia'.padEnd(H.pnlDay)}  ` +
    `${'WR%'.padEnd(H.wr)}  ` +
    `${'Trades'.padEnd(H.trades)}  ` +
    `${'Hold'.padEnd(H.hold)}  ` +
    `Scalp%`
  );
  console.log(separator('─', W));

  rows.forEach((t, idx) => {
    const pnlDailyPct = t.profit_pct != null ? t.profit_pct / days : null;
    const name = t.label
      ? t.label.substring(0, H.wallet - 2).padEnd(H.wallet)
      : t.wallet_address.padEnd(H.wallet);

    console.log(
      `  ${padEnd(String(idx + 1), H.rank)}  ` +
      `${padEnd(name, H.wallet)}  ` +
      `${padEnd(scoreColor(t.follow_score) + fmt(t.follow_score) + C.reset, H.score)}  ` +
      `${padEnd(fmtUsd(t.profit_usd), H.pnlUsd)}  ` +
      `${padEnd(fmtPnl(t.profit_pct), H.pnlPct)}  ` +
      `${padEnd(fmtPnl(pnlDailyPct), H.pnlDay)}  ` +
      `${padEnd(fmtPnl(t.win_rate, '%'), H.wr)}  ` +
      `${String(t.total_trades).padEnd(H.trades)}  ` +
      `${padEnd(fmtHold(t.holding_time_avg_s), H.hold)}  ` +
      `${fmt(t.scalping_rate, 1, '%')}`
    );
  });

  console.log(separator('═', W) + '\n');
  process.exit(0);
}

main().catch((err) => {
  logger.error('top-traders error', { error: (err as Error).message });
  process.exit(1);
});
