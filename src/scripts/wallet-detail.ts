/**
 * wallet-detail.ts
 *
 * Exibe no terminal todas as informações de uma wallet específica:
 *   - Identificação e flags
 *   - Índices e follow score (todos os períodos)
 *   - Posições abertas
 *   - Histórico completo de transações com PnL por trade
 *
 * Uso:
 *   npm run wallet-detail -- 0x163d541d0c385042a85292fdba798fb4f5fd3fed
 *   npm run wallet-detail -- 0x163d... --period 7     → filtra swaps dos últimos 7 dias
 *   npm run wallet-detail -- 0x163d... --limit 50     → limita a 50 swaps exibidos
 */

import { query } from '../database/connection';
import { logger } from '../utils/logger';

// ─── Argumentos ──────────────────────────────────────────────────────────────
const args   = process.argv.slice(2);
const wallet = (args.find(a => a.startsWith('0x')) || '').toLowerCase();
const periodDays = (() => {
  const idx = args.indexOf('--period');
  return idx !== -1 && args[idx + 1] ? parseInt(args[idx + 1], 10) : 0; // 0 = todos
})();
const swapLimit = (() => {
  const idx = args.indexOf('--limit');
  return idx !== -1 && args[idx + 1] ? parseInt(args[idx + 1], 10) : 200;
})();

if (!wallet) {
  console.error('\nUso: npm run wallet-detail -- 0xSEU_ENDERECO [--period 7] [--limit 50]\n');
  process.exit(1);
}

// ─── Cores ANSI ───────────────────────────────────────────────────────────────
const C = {
  reset:   '\x1b[0m', bold: '\x1b[1m', dim: '\x1b[2m',
  green:   '\x1b[32m', red: '\x1b[31m', yellow: '\x1b[33m',
  cyan:    '\x1b[36m', blue: '\x1b[34m', magenta: '\x1b[35m',
  white:   '\x1b[97m', bgBlue: '\x1b[44m', bgGreen: '\x1b[42m',
};

function scoreColor(s: number) { return s>=80?C.green+C.bold:s>=60?C.cyan:s>=40?C.yellow:C.red; }
function pnlColor(v: number)   { return v >= 0 ? C.green : C.red; }
function scoreBar(s: number, w = 20) {
  const f = Math.round((s/100)*w);
  return scoreColor(s) + '█'.repeat(f) + '░'.repeat(w-f) + C.reset;
}
function scoreLabel(s: number) {
  if (s>=80) return `${C.green}${C.bold}EXCELENTE${C.reset}`;
  if (s>=70) return `${C.cyan}MUITO BOM${C.reset}`;
  if (s>=60) return `${C.cyan}BOM      ${C.reset}`;
  if (s>=50) return `${C.yellow}ACEITÁVEL${C.reset}`;
  if (s>=40) return `${C.yellow}FRACO    ${C.reset}`;
  return `${C.red}NÃO REC. ${C.reset}`;
}
function fmt(v: number|null|undefined, d=2, suf='') {
  if (v===null||v===undefined) return `${C.dim}N/A${C.reset}`;
  return `${v.toFixed(d)}${suf}`;
}
function fmtPnl(v: number|null|undefined, suf='%') {
  if (v===null||v===undefined) return `${C.dim}N/A${C.reset}`;
  return `${pnlColor(v)}${v>=0?'+':''}${v.toFixed(2)}${suf}${C.reset}`;
}
function fmtUsd(v: number|null|undefined) {
  if (v===null||v===undefined) return `${C.dim}N/A${C.reset}`;
  const abs = Math.abs(v);
  const str = abs>=1e6?`${(v/1e6).toFixed(2)}M`:abs>=1000?`${(v/1000).toFixed(2)}K`:v.toFixed(2);
  return `${pnlColor(v)}${v>=0?'+':''}$${str}${C.reset}`;
}
function fmtHold(sec: number|null|undefined) {
  if (!sec) return `${C.dim}N/A${C.reset}`;
  const s = Math.round(Number(sec));
  if (s<60)    return `${s}s`;
  if (s<3600)  return `${Math.floor(s/60)}m ${s%60}s`;
  if (s<86400) return `${Math.floor(s/3600)}h ${Math.floor((s%3600)/60)}m`;
  return `${Math.floor(s/86400)}d ${Math.floor((s%86400)/3600)}h`;
}
function flag(v: number|boolean, label: string) {
  return v ? `${C.red}${C.bold}[${label}]${C.reset}` : `${C.dim}[${label}]${C.reset}`;
}
function sep(c='─', w=130) { return C.dim + c.repeat(w) + C.reset; }
function padEnd(str: string, len: number) {
  const clean = str.replace(/\x1b\[[0-9;]*m/g, '');
  return str + ' '.repeat(Math.max(0, len - clean.length));
}

// ─── Tipos ────────────────────────────────────────────────────────────────────
interface WalletRow {
  address: string; label: string|null;
  first_seen: string; last_seen: string; total_transactions: number;
  is_disqualified: number;
  flag_scalper: number; flag_bundler: number; flag_sybil: number; flag_creator_funded: number;
  scalper_copiability_index: number;
}
interface MetricsRow {
  period: string; follow_score: number;
  profit_usd: number; profit_pct: number; win_rate: number;
  total_trades: number; wins: number; losses: number; trades_per_day: number;
  holding_time_avg_s: number; scalping_rate: number; long_trade_rate_pct: number;
  followability_final: number; followability_hold_score: number;
  followability_volume_score: number; followability_liq_score: number;
  consistency_final: number; consistency_wr_stability: number;
  consistency_pnl_stability: number; consistency_diversification: number;
  monthly_wr_cv: number; profitable_months_ratio: number; diversification_ratio: number;
  pnl_final: number; pnl_relative_score: number; pnl_profit_factor: number;
  pnl_pf_score: number; gross_profit_usd: number; gross_loss_usd: number;
  p90_benchmark_usd: number; win_rate_score: number;
  unique_tokens_traded: number; best_trade_pnl: number; worst_trade_pnl: number;
  has_sufficient_data: number; period_start: string; last_updated: string;
}
interface PositionRow {
  token_address: string; token_symbol: string;
  base_token_symbol: string; qty_open: number;
  cost_basis_base: number; avg_cost_base: number; updated_at: string;
}
interface SwapRow {
  id: number; timestamp: string; tx_hash: string;
  token_in_symbol: string; token_in_amount: string;
  token_out_symbol: string; token_out_amount: string;
  value_usd: number; swap_type: string|null;
  pnl_base: number|null; pnl_base_symbol: string|null;
  pnl_pct: number|null; is_win: number|null;
  holding_time_s: number|null; is_long_trade: number|null;
  dex_address: string;
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  const W = 130;
  console.log('\n' + sep('═', W));
  console.log(`${C.bgBlue}${C.white}${C.bold}  🔍  WALLET DETAIL — KOLSCAN${' '.repeat(W-30)}${C.reset}`);
  console.log(`${C.dim}  ${wallet}${C.reset}`);
  console.log(sep('═', W));

  // ── 1. Dados da wallet ──────────────────────────────────────────────────────
  const [w] = await query<WalletRow>(
    `SELECT address, label, first_seen, last_seen, total_transactions,
            is_disqualified, flag_scalper, flag_bundler, flag_sybil,
            flag_creator_funded, scalper_copiability_index
     FROM wallets WHERE address = ?`, [wallet]
  );

  if (!w) {
    console.log(`\n${C.red}  Wallet não encontrada no banco de dados.${C.reset}`);
    console.log(`  Verifique se o endereço está correto e se o indexer já processou transações desta wallet.\n`);
    process.exit(0);
  }

  console.log(`\n  ${C.bold}${C.white}${w.label || wallet}${C.reset}`);
  if (w.label) console.log(`  ${C.dim}${wallet}${C.reset}`);
  console.log(
    `  Primeira atividade: ${C.cyan}${w.first_seen}${C.reset}  ` +
    `Última atividade: ${C.cyan}${w.last_seen}${C.reset}  ` +
    `Total de transações: ${C.bold}${w.total_transactions}${C.reset}`
  );
  console.log(
    `  Flags: ${flag(w.flag_scalper,'SCALPER')} ${flag(w.flag_bundler,'BUNDLER')} ` +
    `${flag(w.flag_sybil,'SYBIL')} ${flag(w.flag_creator_funded,'CREATOR_FUNDED')} ` +
    `${w.is_disqualified ? `${C.red}${C.bold}[DESQUALIFICADO]${C.reset}` : `${C.dim}[ATIVO]${C.reset}`}`
  );
  if (w.scalper_copiability_index > 0) {
    console.log(`  Scalper Copiability Index: ${C.yellow}${fmt(w.scalper_copiability_index, 3)}${C.reset}`);
  }

  // ── 2. Métricas por período ─────────────────────────────────────────────────
  const metrics = await query<MetricsRow>(
    `SELECT period, follow_score, profit_usd, profit_pct, win_rate,
            total_trades, wins, losses, trades_per_day,
            holding_time_avg_s, scalping_rate, long_trade_rate_pct,
            followability_final, followability_hold_score,
            followability_volume_score, followability_liq_score,
            consistency_final, consistency_wr_stability,
            consistency_pnl_stability, consistency_diversification,
            monthly_wr_cv, profitable_months_ratio, diversification_ratio,
            pnl_final, pnl_relative_score, pnl_profit_factor,
            pnl_pf_score, gross_profit_usd, gross_loss_usd,
            p90_benchmark_usd, win_rate_score,
            unique_tokens_traded, best_trade_pnl, worst_trade_pnl,
            has_sufficient_data,
            DATE_FORMAT(period_start,'%Y-%m-%d') AS period_start,
            DATE_FORMAT(last_updated,'%Y-%m-%d %H:%i') AS last_updated
     FROM kol_metrics
     WHERE wallet_address = ?
     ORDER BY FIELD(period,'daily','weekly','monthly','all_time')`,
    [wallet]
  );

  const periodLabel: Record<string,string> = {
    daily: '1 dia', weekly: '7 dias', monthly: '30 dias', all_time: 'Todo o período'
  };

  if (metrics.length === 0) {
    console.log(`\n${C.yellow}  Métricas ainda não calculadas. Rode: npm run recalculate-all${C.reset}`);
  } else {
    console.log('\n' + sep('─', W));
    console.log(`  ${C.bold}${C.cyan}📊  ÍNDICES E FOLLOW SCORE${C.reset}`);

    metrics.forEach(m => {
      const lbl = periodLabel[m.period] || m.period;
      const score = Number(m.follow_score);
      console.log('\n' + sep('·', W));
      console.log(
        `  ${C.bold}Período: ${C.yellow}${lbl}${C.reset}${C.bold}${C.reset}  ` +
        `${C.dim}(desde ${m.period_start} | atualizado: ${m.last_updated})${C.reset}`
      );
      console.log(
        `  ${scoreLabel(score)}  ${scoreBar(score, 25)}  ` +
        `${scoreColor(score)}${C.bold}Follow Score: ${fmt(score)}/100${C.reset}` +
        (m.has_sufficient_data ? '' : `  ${C.yellow}[dados insuficientes]${C.reset}`)
      );

      // PnL e Win Rate
      console.log(
        `\n  ${C.bold}PnL Total:${C.reset} ${padEnd(fmtUsd(m.profit_usd),14)}` +
        `  ${C.bold}PnL Médio/trade:${C.reset} ${padEnd(fmtPnl(m.profit_pct),12)}` +
        `  ${C.bold}Win Rate:${C.reset} ${fmtPnl(m.win_rate,'%')}`
      );
      console.log(
        `  ${C.bold}Gross Profit:${C.reset} ${padEnd(fmtUsd(m.gross_profit_usd),14)}` +
        `  ${C.bold}Gross Loss:${C.reset} ${padEnd(fmtUsd(m.gross_loss_usd),14)}` +
        `  ${C.bold}Profit Factor:${C.reset} ${fmt(m.pnl_profit_factor,2)}`
      );
      console.log(
        `  ${C.bold}Melhor trade:${C.reset} ${padEnd(fmtPnl(m.best_trade_pnl),12)}` +
        `  ${C.bold}Pior trade:${C.reset} ${padEnd(fmtPnl(m.worst_trade_pnl),12)}` +
        `  ${C.bold}P90 benchmark:${C.reset} ${fmtUsd(m.p90_benchmark_usd)}`
      );

      // Trades
      console.log(
        `\n  ${C.bold}Trades:${C.reset} ${m.total_trades}  ` +
        `${C.green}✓ ${m.wins}${C.reset}  ${C.red}✗ ${m.losses}${C.reset}  ` +
        `  ${C.bold}Trades/dia:${C.reset} ${fmt(m.trades_per_day)}` +
        `  ${C.bold}Tokens únicos:${C.reset} ${m.unique_tokens_traded ?? 'N/A'}` +
        `  ${C.bold}Diversif.:${C.reset} ${fmt(m.diversification_ratio?m.diversification_ratio*100:null,1,'%')}`
      );
      console.log(
        `  ${C.bold}Hold médio:${C.reset} ${padEnd(fmtHold(m.holding_time_avg_s),12)}` +
        `  ${C.bold}Scalping:${C.reset} ${padEnd(fmt(m.scalping_rate,1,'%'),8)}` +
        `  ${C.bold}Long trades:${C.reset} ${fmt(m.long_trade_rate_pct,1,'%')}`
      );

      // Componentes
      console.log(`\n  ${C.dim}── Componentes ──────────────────────────────────────────────────────────────${C.reset}`);
      console.log(
        `  ${C.magenta}Followability ${C.reset}${scoreBar(m.followability_final,15)} ` +
        `${padEnd(fmt(m.followability_final)+'/100',10)}  ` +
        `${C.dim}Hold: ${fmt(m.followability_hold_score,1).padEnd(6)} | Vol: ${fmt(m.followability_volume_score,1).padEnd(6)} | Liq: ${fmt(m.followability_liq_score,1)}${C.reset}`
      );
      console.log(
        `  ${C.blue}Consistência  ${C.reset}${scoreBar(m.consistency_final,15)} ` +
        `${padEnd(fmt(m.consistency_final)+'/100',10)}  ` +
        `${C.dim}WR Stab: ${fmt(m.consistency_wr_stability,1).padEnd(6)} | PnL Stab: ${fmt(m.consistency_pnl_stability,1).padEnd(6)} | Divers: ${fmt(m.consistency_diversification,1)}${C.reset}`
      );
      console.log(
        `  ${C.green}PnL Score     ${C.reset}${scoreBar(m.pnl_final,15)} ` +
        `${padEnd(fmt(m.pnl_final)+'/100',10)}  ` +
        `${C.dim}Rel: ${fmt(m.pnl_relative_score,1).padEnd(6)} | PF Score: ${fmt(m.pnl_pf_score,1)}${C.reset}`
      );
      console.log(
        `  ${C.yellow}Win Rate      ${C.reset}${scoreBar(m.win_rate_score,15)} ` +
        `${padEnd(fmt(m.win_rate_score)+'/100',10)}  ` +
        `${C.dim}WR CV: ${fmt(m.monthly_wr_cv,1,'%')} | Meses lucrativos: ${fmt(m.profitable_months_ratio,1,'%')}${C.reset}`
      );
    });
  }

  // ── 3. Posições abertas ─────────────────────────────────────────────────────
  const positions = await query<PositionRow>(
    `SELECT p.token_address, COALESCE(t.symbol, p.token_address) AS token_symbol,
            p.base_token_symbol, p.qty_open, p.cost_basis_base,
            p.avg_cost_base, DATE_FORMAT(p.updated_at,'%Y-%m-%d %H:%i') AS updated_at
     FROM positions p
     LEFT JOIN tokens t ON t.address = p.token_address
     WHERE p.wallet_address = ? AND p.qty_open > 0
     ORDER BY p.updated_at DESC`,
    [wallet]
  );

  console.log('\n' + sep('─', W));
  console.log(`  ${C.bold}${C.cyan}📂  POSIÇÕES ABERTAS (${positions.length})${C.reset}`);

  if (positions.length === 0) {
    console.log(`  ${C.dim}Nenhuma posição aberta.${C.reset}`);
  } else {
    console.log(
      `\n  ${'Token'.padEnd(12)}  ${'Endereço'.padEnd(44)}  ${'Qty aberta'.padEnd(18)}  ` +
      `${'Custo total'.padEnd(18)}  ${'Custo médio'.padEnd(18)}  Moeda base`
    );
    console.log(sep('·', W));
    positions.forEach(p => {
      console.log(
        `  ${p.token_symbol.padEnd(12)}  ${p.token_address.padEnd(44)}  ` +
        `${String(Number(p.qty_open).toFixed(4)).padEnd(18)}  ` +
        `${String(Number(p.cost_basis_base).toFixed(6)).padEnd(18)}  ` +
        `${String(Number(p.avg_cost_base).toFixed(8)).padEnd(18)}  ${p.base_token_symbol}`
      );
    });
  }

  // ── 4. Histórico de swaps ───────────────────────────────────────────────────
  const whereTime = periodDays > 0
    ? `AND se.timestamp >= DATE_SUB(NOW(), INTERVAL ${periodDays} DAY)`
    : '';

  const swaps = await query<SwapRow>(
    `SELECT se.id, DATE_FORMAT(se.timestamp,'%Y-%m-%d %H:%i:%S') AS timestamp,
            se.tx_hash,
            se.token_in_symbol, se.token_in_amount,
            se.token_out_symbol, se.token_out_amount,
            se.value_usd, se.swap_type,
            se.pnl_base, se.pnl_base_symbol, se.pnl_pct,
            se.is_win, se.holding_time_s, se.is_long_trade,
            se.dex_address
     FROM swap_events se
     WHERE se.wallet_address = ? ${whereTime}
     ORDER BY se.timestamp DESC
     LIMIT ${swapLimit}`,
    [wallet]
  );

  const totalSwaps = await query<{total:number}>(
    `SELECT COUNT(*) AS total FROM swap_events WHERE wallet_address = ? ${whereTime}`,
    [wallet]
  );

  const total = totalSwaps[0]?.total ?? 0;
  const periodLabel2 = periodDays > 0 ? ` (últimos ${periodDays} dias)` : '';
  console.log('\n' + sep('─', W));
  console.log(
    `  ${C.bold}${C.cyan}📋  HISTÓRICO DE SWAPS${periodLabel2}${C.reset}  ` +
    `${C.dim}Exibindo ${swaps.length} de ${total} | --limit N para mais${C.reset}`
  );

  if (swaps.length === 0) {
    console.log(`  ${C.dim}Nenhum swap encontrado.${C.reset}`);
  } else {
    // Cabeçalho
    console.log(
      `\n  ${'Data/Hora'.padEnd(20)}  ${'Tipo'.padEnd(5)}  ` +
      `${'De'.padEnd(10)}  ${'Para'.padEnd(10)}  ` +
      `${'Valor USD'.padEnd(12)}  ${'PnL'.padEnd(14)}  ${'PnL%'.padEnd(10)}  ` +
      `${'Hold'.padEnd(10)}  ${'W/L'.padEnd(4)}  TX Hash`
    );
    console.log(sep('·', W));

    swaps.forEach(s => {
      const typeStr = s.swap_type === 'buy'  ? `${C.green}BUY  ${C.reset}` :
                      s.swap_type === 'sell' ? `${C.red}SELL ${C.reset}` :
                                               `${C.dim}SWAP ${C.reset}`;

      const pnlStr = s.pnl_base !== null
        ? `${pnlColor(s.pnl_base)}${s.pnl_base>=0?'+':''}${Number(s.pnl_base).toFixed(4)} ${s.pnl_base_symbol}${C.reset}`
        : `${C.dim}—${C.reset}`;

      const pnlPctStr = s.pnl_pct !== null
        ? fmtPnl(s.pnl_pct)
        : `${C.dim}—${C.reset}`;

      const winStr = s.is_win === 1 ? `${C.green}WIN ${C.reset}` :
                     s.is_win === 0 ? `${C.red}LOSS${C.reset}` :
                                      `${C.dim}—   ${C.reset}`;

      const txShort = `${s.tx_hash.substring(0,10)}...${s.tx_hash.substring(s.tx_hash.length-6)}`;

      console.log(
        `  ${s.timestamp.padEnd(20)}  ${padEnd(typeStr,5)}  ` +
        `${s.token_in_symbol.substring(0,10).padEnd(10)}  ${s.token_out_symbol.substring(0,10).padEnd(10)}  ` +
        `${padEnd(fmtUsd(s.value_usd),12)}  ${padEnd(pnlStr,14)}  ${padEnd(pnlPctStr,10)}  ` +
        `${padEnd(fmtHold(s.holding_time_s),10)}  ${winStr}  ${txShort}`
      );
    });
  }

  // ── 5. Resumo estatístico dos swaps ────────────────────────────────────────
  const stats = await query<{
    total_swaps: number; total_buys: number; total_sells: number; total_swaps_mm: number;
    wins: number; losses: number; avg_pnl_pct: number; total_pnl_usd: number;
    avg_hold: number; min_hold: number; max_hold: number;
  }>(
    `SELECT COUNT(*) AS total_swaps,
            SUM(swap_type='buy')  AS total_buys,
            SUM(swap_type='sell') AS total_sells,
            SUM(swap_type='swap') AS total_swaps_mm,
            SUM(is_win=1) AS wins,
            SUM(is_win=0) AS losses,
            AVG(pnl_pct)  AS avg_pnl_pct,
            SUM(COALESCE(pnl_base * COALESCE((SELECT price_usd FROM tokens WHERE address=se.pnl_base_token LIMIT 1),1),0)) AS total_pnl_usd,
            AVG(holding_time_s) AS avg_hold,
            MIN(holding_time_s) AS min_hold,
            MAX(holding_time_s) AS max_hold
     FROM swap_events se WHERE wallet_address = ? ${whereTime}`,
    [wallet]
  );

  if (stats[0]) {
    const st = stats[0];
    console.log('\n' + sep('─', W));
    console.log(`  ${C.bold}${C.cyan}📈  ESTATÍSTICAS GERAIS${periodLabel2}${C.reset}`);
    console.log(
      `\n  Total de swaps: ${C.bold}${st.total_swaps}${C.reset}` +
      `  ${C.green}Compras: ${st.total_buys}${C.reset}` +
      `  ${C.red}Vendas: ${st.total_sells}${C.reset}` +
      `  ${C.dim}Swaps meme→meme: ${st.total_swaps_mm}${C.reset}`
    );
    console.log(
      `  Wins: ${C.green}${st.wins}${C.reset}  Losses: ${C.red}${st.losses}${C.reset}` +
      `  Win Rate: ${fmtPnl(st.wins && st.losses !== null ? (Number(st.wins)/(Number(st.wins)+Number(st.losses)))*100 : null,'%')}` +
      `  PnL médio/trade: ${fmtPnl(st.avg_pnl_pct)}` +
      `  PnL total USD: ${fmtUsd(st.total_pnl_usd)}`
    );
    console.log(
      `  Hold médio: ${fmtHold(st.avg_hold)}` +
      `  Hold mínimo: ${fmtHold(st.min_hold)}` +
      `  Hold máximo: ${fmtHold(st.max_hold)}`
    );
  }

  console.log('\n' + sep('═', W) + '\n');
  process.exit(0);
}

main().catch(err => {
  logger.error('wallet-detail error', { error: (err as Error).message });
  process.exit(1);
});
