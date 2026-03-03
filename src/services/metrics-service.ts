/**
 * metrics-service.ts — v2.0
 *
 * Follow Score com 4 componentes (metodologia KOLSCAN v2):
 *   Followability  (40%) — Hold Time (50%) + Volume (30%) + Liquidity (20%)
 *   Consistência   (25%) — WR Stability (40%) + PnL Stability (35%) + Diversification (25%)
 *   PnL            (20%) — PnL vs P90 (60%) + Profit Factor (40%)
 *   Win Rate       (15%) — Curva linear 0–70%+
 */

import { query, execute } from '../database/connection';
import { logger } from '../utils/logger';
import { config } from '../config';
import type { LeaderboardEntry, KolDetails } from '../types';

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function formatHoldingTime(seconds: number | null): string | null {
  if (seconds === null || seconds === undefined) return null;
  const s = Math.round(Number(seconds));
  if (s < 60)    return `${s}s`;
  if (s < 3600)  return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86400) { const h = Math.floor(s/3600); const m = Math.floor((s%3600)/60); return `${h}h ${m}m`; }
  const d = Math.floor(s/86400); const h = Math.floor((s%86400)/3600); return `${d}d ${h}h`;
}

function followScoreLabel(score: number): string {
  if (score >= 80) return 'Excelente — altamente recomendado seguir';
  if (score >= 70) return 'Muito Bom — excelente para seguir';
  if (score >= 60) return 'Bom — vale a pena acompanhar';
  if (score >= 50) return 'Aceitável — siga com atenção';
  if (score >= 40) return 'Fraco — scalper ou baixo win rate';
  return 'Não recomendado — alto risco';
}

// ─────────────────────────────────────────────────────────────────────────────
// Cálculo do Follow Score
// ─────────────────────────────────────────────────────────────────────────────

interface ScoreComponents {
  followability_hold_score: number; followability_volume_score: number;
  followability_liq_score: number; followability_final: number;
  consistency_wr_stability: number; consistency_pnl_stability: number;
  consistency_diversification: number; consistency_final: number;
  pnl_relative_score: number; pnl_profit_factor: number;
  pnl_pf_score: number; pnl_final: number;
  win_rate_score: number; follow_score: number;
  trades_per_day: number; gross_profit_usd: number; gross_loss_usd: number;
  monthly_wr_cv: number; profitable_months_ratio: number;
  diversification_ratio: number; p90_benchmark_usd: number;
  total_trades: number; has_sufficient_data: number;
}

async function computeFollowScore(wallet: string, periodDays: number): Promise<ScoreComponents> {
  const threshold = config.indexer.scalpingThresholdSeconds;
  const minTrades = config.indexer.minTradesForKol;

  // BLOCO 1: FOLLOWABILITY
  const holdRows = await query<{ avg_hold: number; total: number; scalping_rate: number }>(
    `SELECT COALESCE(AVG(holding_time_s),0) AS avg_hold, COUNT(*) AS total,
            COALESCE(SUM(CASE WHEN holding_time_s < ${threshold} THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0)*100,0) AS scalping_rate
     FROM swap_events WHERE wallet_address=? AND holding_time_s IS NOT NULL AND timestamp>=DATE_SUB(NOW(),INTERVAL ? DAY)`,
    [wallet, periodDays]
  );
  const avgHold = Number(holdRows[0]?.avg_hold)||0;
  const totalTrades = Number(holdRows[0]?.total)||0;
  const scalpingRate = Number(holdRows[0]?.scalping_rate)||0;
  const tradesPerDay = periodDays>0 ? totalTrades/periodDays : 0;

  let holdScore = avgHold<60 ? 0 : avgHold<=300 ? 20+(avgHold-60)/240*40 : avgHold<=1800 ? 60+(avgHold-300)/1500*25 : 100;
  holdScore = holdScore*(1-(scalpingRate/100)*0.5);
  const volumeScore = tradesPerDay<1 ? 100 : tradesPerDay<=5 ? 100-(tradesPerDay-1)/4*30 : tradesPerDay<=20 ? 70-(tradesPerDay-5)/15*30 : 0;
  const liqScore = 75;
  const followability = holdScore*0.50 + volumeScore*0.30 + liqScore*0.20;

  // BLOCO 2: CONSISTÊNCIA
  const wrCvRows = await query<{ wr_cv: number }>(
    `SELECT COALESCE((STDDEV(monthly_wr)/NULLIF(AVG(monthly_wr),0))*100,100) AS wr_cv
     FROM (SELECT DATE_FORMAT(timestamp,'%Y-%m') AS month,
                  SUM(CASE WHEN is_win=1 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0)*100 AS monthly_wr
           FROM swap_events WHERE wallet_address=? AND is_long_trade=1 AND timestamp>=DATE_SUB(NOW(),INTERVAL 6 MONTH)
           GROUP BY month) m`,
    [wallet]
  );
  const wrCv = Number(wrCvRows[0]?.wr_cv)||100;

  const pmRows = await query<{ pm: number }>(
    `SELECT COALESCE(SUM(CASE WHEN monthly_pnl>0 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0)*100,0) AS pm
     FROM (SELECT DATE_FORMAT(timestamp,'%Y-%m') AS month, SUM(pnl) AS monthly_pnl
           FROM swap_events WHERE wallet_address=? AND is_long_trade=1 AND timestamp>=DATE_SUB(NOW(),INTERVAL 6 MONTH)
           GROUP BY month) m`,
    [wallet]
  );
  const profitableMonths = Number(pmRows[0]?.pm)||0;

  const divRows = await query<{ dr: number }>(
    `SELECT COALESCE(COUNT(DISTINCT token_out_address)/NULLIF(COUNT(*),0),0) AS dr
     FROM swap_events WHERE wallet_address=? AND is_long_trade=1 AND timestamp>=DATE_SUB(NOW(),INTERVAL ? DAY)`,
    [wallet, periodDays]
  );
  const diversRatio = Number(divRows[0]?.dr)||0;

  const wrStability = wrCv<10 ? 100 : wrCv<=25 ? 100-(wrCv-10)/15*30 : wrCv<=50 ? 70-(wrCv-25)/25*40 : 0;
  const pnlStability = profitableMonths>=100 ? 100 : profitableMonths>=80 ? 80+(profitableMonths-80)/20*20 : profitableMonths>=60 ? 60+(profitableMonths-60)/20*20 : profitableMonths>=40 ? 40+(profitableMonths-40)/20*20 : profitableMonths/40*40;
  const diversScore = diversRatio<0.10 ? 20 : diversRatio<=0.30 ? 20+(diversRatio-0.10)/0.20*50 : diversRatio<=0.50 ? 70+(diversRatio-0.30)/0.20*20 : 100;
  const consistency = wrStability*0.40 + pnlStability*0.35 + diversScore*0.25;

  // BLOCO 3: PnL
  // Usa pnl_base (PnL real na moeda base) apenas em swaps do tipo 'sell'.
  // O pnl_base é null em compras e em swaps meme→meme, portanto só conta
  // quando a posição foi efetivamente fechada contra uma moeda base.
  // Para o leaderboard (que precisa de USD), converte via preço atual do token base.
  // Como o preço histórico não está armazenado, usamos pnl (USD) como fallback
  // quando pnl_base não está disponível (dados anteriores à migration 9).
  const pnlRows = await query<{ total_pnl: number; gross_profit: number; gross_loss: number }>(
    `SELECT
       COALESCE(SUM(
         CASE WHEN pnl_base IS NOT NULL THEN pnl_base * COALESCE(
           (SELECT price_usd FROM tokens WHERE address = se.pnl_base_token LIMIT 1), 1
         ) ELSE pnl END
       ), 0) AS total_pnl,
       COALESCE(SUM(CASE WHEN is_win=1 THEN
         CASE WHEN pnl_base IS NOT NULL THEN pnl_base * COALESCE(
           (SELECT price_usd FROM tokens WHERE address = se.pnl_base_token LIMIT 1), 1
         ) ELSE pnl END
       ELSE 0 END), 0) AS gross_profit,
       COALESCE(SUM(CASE WHEN is_win=0 THEN ABS(
         CASE WHEN pnl_base IS NOT NULL THEN pnl_base * COALESCE(
           (SELECT price_usd FROM tokens WHERE address = se.pnl_base_token LIMIT 1), 1
         ) ELSE pnl END
       ) ELSE 0 END), 0) AS gross_loss
     FROM swap_events se
     WHERE se.wallet_address=?
       AND se.is_long_trade=1
       AND (se.swap_type='sell' OR se.swap_type IS NULL)
       AND se.timestamp>=DATE_SUB(NOW(),INTERVAL ? DAY)`,
    [wallet, periodDays]
  );
  const totalPnl = Number(pnlRows[0]?.total_pnl)||0;
  const grossProfit = Number(pnlRows[0]?.gross_profit)||0;
  const grossLoss = Number(pnlRows[0]?.gross_loss)||0;

  // P90 do pool completo (usando mesma lógica de pnl_base com fallback para pnl)
  const allPnlRows = await query<{ pnl_sum: number }>(
    `SELECT SUM(
       CASE WHEN pnl_base IS NOT NULL THEN pnl_base * COALESCE(
         (SELECT price_usd FROM tokens WHERE address = se.pnl_base_token LIMIT 1), 1
       ) ELSE pnl END
     ) AS pnl_sum
     FROM swap_events se
     WHERE se.is_long_trade=1
       AND (se.swap_type='sell' OR se.swap_type IS NULL)
       AND se.timestamp>=DATE_SUB(NOW(),INTERVAL ? DAY)
     GROUP BY se.wallet_address ORDER BY pnl_sum`,
    [periodDays]
  );
  let p90Pnl = 1;
  if (allPnlRows.length>0) {
    const idx = Math.floor(allPnlRows.length*0.9);
    p90Pnl = Number(allPnlRows[Math.min(idx,allPnlRows.length-1)]?.pnl_sum)||1;
    if (p90Pnl===0) p90Pnl=1;
  }

  // Cap em 9999 para evitar out-of-range na coluna DECIMAL(10,2)
  const rawProfitFactor = grossLoss===0 ? (grossProfit>0 ? 9999 : 0) : grossProfit/grossLoss;
  const profitFactor = Math.min(9999, Math.max(0, rawProfitFactor));
  const pnlRatio = totalPnl/Math.abs(p90Pnl);
  const pnlRelScore = pnlRatio>=1 ? 100 : pnlRatio>=0.5 ? 50+(pnlRatio-0.5)/0.5*50 : pnlRatio>=0.1 ? 10+(pnlRatio-0.1)/0.4*40 : pnlRatio>0 ? pnlRatio/0.1*10 : 0;
  const pfScore = profitFactor>=3 ? 100 : profitFactor>=2 ? 80+(profitFactor-2)*20 : profitFactor>=1.5 ? 60+(profitFactor-1.5)/0.5*20 : profitFactor>=1 ? 40+(profitFactor-1)/0.5*20 : 0;
  const pnlFinal = pnlRelScore*0.60 + pfScore*0.40;

  // BLOCO 4: WIN RATE
  const wrRows = await query<{ wr: number }>(
    `SELECT COALESCE(SUM(CASE WHEN is_win=1 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0)*100,0) AS wr
     FROM swap_events WHERE wallet_address=? AND is_long_trade=1 AND timestamp>=DATE_SUB(NOW(),INTERVAL ? DAY)`,
    [wallet, periodDays]
  );
  const winRate = Number(wrRows[0]?.wr)||0;
  const wrScore = winRate>=70 ? 100 : winRate>=60 ? 80+(winRate-60)/10*20 : winRate>=50 ? 60+(winRate-50)/10*20 : winRate>=40 ? 40+(winRate-40)/10*20 : winRate>=30 ? 20+(winRate-30)/10*20 : (winRate/30)*20;

  const followScore = Math.min(100, Math.max(0, followability*0.40 + consistency*0.25 + pnlFinal*0.20 + wrScore*0.15));
  const r = (v: number) => Math.round(v*100)/100;

  return {
    followability_hold_score: r(holdScore), followability_volume_score: r(volumeScore),
    followability_liq_score: liqScore, followability_final: r(followability),
    consistency_wr_stability: r(wrStability), consistency_pnl_stability: r(pnlStability),
    consistency_diversification: r(diversScore), consistency_final: r(consistency),
    pnl_relative_score: r(pnlRelScore), pnl_profit_factor: r(profitFactor),
    pnl_pf_score: r(pfScore), pnl_final: r(pnlFinal),
    win_rate_score: r(wrScore), follow_score: r(followScore),
    trades_per_day: r(tradesPerDay), gross_profit_usd: grossProfit, gross_loss_usd: grossLoss,
    monthly_wr_cv: r(wrCv), profitable_months_ratio: r(profitableMonths),
    diversification_ratio: Math.round(diversRatio*10000)/10000, p90_benchmark_usd: p90Pnl,
    total_trades: totalTrades, has_sufficient_data: totalTrades>=minTrades ? 1 : 0,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Atualizar métricas de uma wallet
// ─────────────────────────────────────────────────────────────────────────────

export async function updateKolMetrics(wallet: string): Promise<void> {
  const periods = [
    { label: 'daily', days: 1 }, { label: 'weekly', days: 7 },
    { label: 'monthly', days: 30 }, { label: 'all_time', days: 3650 },
  ];

  for (const period of periods) {
    try {
      const c = await computeFollowScore(wallet, period.days);
      const basicRows = await query<{
        wins: number; losses: number; total_trades: number; profit_usd: number; win_rate: number;
        holding_time_avg_s: number; scalping_rate: number; long_trade_rate: number;
        best_trade_pnl: number; worst_trade_pnl: number; total_invested_usd: number;
      }>(
        `SELECT
                SUM(CASE WHEN is_win=1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN is_win=0 THEN 1 ELSE 0 END) AS losses,
                COUNT(*) AS total_trades,
                -- profit_usd: usa pnl_base convertido para USD quando disponível (swap_type='sell'),
                -- caso contrário usa pnl legado (dados anteriores à migration 9)
                COALESCE(SUM(
                  CASE WHEN pnl_base IS NOT NULL THEN pnl_base * COALESCE(
                    (SELECT price_usd FROM tokens WHERE address = se.pnl_base_token LIMIT 1), 1
                  ) ELSE pnl END
                ), 0) AS profit_usd,
                COALESCE(SUM(CASE WHEN is_win=1 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0)*100,0) AS win_rate,
                COALESCE(AVG(holding_time_s),0) AS holding_time_avg_s,
                COALESCE(SUM(CASE WHEN is_long_trade=0 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0)*100,0) AS scalping_rate,
                COALESCE(AVG(is_long_trade),0) AS long_trade_rate,
                COALESCE(MAX(pnl),0) AS best_trade_pnl, COALESCE(MIN(pnl),0) AS worst_trade_pnl,
                COALESCE(SUM(CASE WHEN swap_type='buy' AND value_usd>0 THEN value_usd
                               WHEN swap_type IS NULL AND value_usd>0 THEN value_usd
                               ELSE 0 END),1) AS total_invested_usd
         FROM swap_events se WHERE se.wallet_address=? AND is_long_trade IS NOT NULL AND timestamp>=DATE_SUB(NOW(),INTERVAL ? DAY)`,
        [wallet, period.days]
      );
      const b = basicRows[0];
      if (!b) continue;
      // Cap em 999999.9999 para evitar out-of-range na coluna DECIMAL(20,4)
      const rawProfitPct = Number(b.total_invested_usd)>0 ? Math.round((Number(b.profit_usd)/Number(b.total_invested_usd))*10000)/100 : 0;
      const profitPct = Math.min(999999.9999, Math.max(-999999.9999, rawProfitPct));
      const periodStart = new Date(Date.now()-period.days*86400*1000);

      await execute(
        `INSERT INTO kol_metrics (
           wallet_address,period,period_start,period_end,
           wins,losses,total_trades,profit_usd,win_rate,
           holding_time_avg_s,scalping_rate,long_trade_rate_pct,
           best_trade_pnl,worst_trade_pnl,profit_pct,
           followability_hold_score,followability_volume_score,followability_liq_score,followability_final,
           consistency_wr_stability,consistency_pnl_stability,consistency_diversification,consistency_final,
           pnl_relative_score,pnl_profit_factor,pnl_pf_score,pnl_final,
           win_rate_score,follow_score,
           trades_per_day,gross_profit_usd,gross_loss_usd,
           monthly_wr_cv,profitable_months_ratio,diversification_ratio,p90_benchmark_usd,
           has_sufficient_data,last_updated
         ) VALUES (?,?,?,NOW(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())
         ON DUPLICATE KEY UPDATE
           wins=VALUES(wins),losses=VALUES(losses),total_trades=VALUES(total_trades),
           profit_usd=VALUES(profit_usd),win_rate=VALUES(win_rate),
           holding_time_avg_s=VALUES(holding_time_avg_s),scalping_rate=VALUES(scalping_rate),
           long_trade_rate_pct=VALUES(long_trade_rate_pct),
           best_trade_pnl=VALUES(best_trade_pnl),worst_trade_pnl=VALUES(worst_trade_pnl),
           profit_pct=VALUES(profit_pct),
           followability_hold_score=VALUES(followability_hold_score),
           followability_volume_score=VALUES(followability_volume_score),
           followability_liq_score=VALUES(followability_liq_score),
           followability_final=VALUES(followability_final),
           consistency_wr_stability=VALUES(consistency_wr_stability),
           consistency_pnl_stability=VALUES(consistency_pnl_stability),
           consistency_diversification=VALUES(consistency_diversification),
           consistency_final=VALUES(consistency_final),
           pnl_relative_score=VALUES(pnl_relative_score),
           pnl_profit_factor=VALUES(pnl_profit_factor),
           pnl_pf_score=VALUES(pnl_pf_score),pnl_final=VALUES(pnl_final),
           win_rate_score=VALUES(win_rate_score),follow_score=VALUES(follow_score),
           trades_per_day=VALUES(trades_per_day),
           gross_profit_usd=VALUES(gross_profit_usd),gross_loss_usd=VALUES(gross_loss_usd),
           monthly_wr_cv=VALUES(monthly_wr_cv),profitable_months_ratio=VALUES(profitable_months_ratio),
           diversification_ratio=VALUES(diversification_ratio),p90_benchmark_usd=VALUES(p90_benchmark_usd),
           has_sufficient_data=VALUES(has_sufficient_data),period_end=NOW(),last_updated=NOW()`,
        [
          wallet, period.label, periodStart,
          Number(b.wins), Number(b.losses), Number(b.total_trades), Number(b.profit_usd), Number(b.win_rate),
          Number(b.holding_time_avg_s), Number(b.scalping_rate), Number(b.long_trade_rate)*100,
          Number(b.best_trade_pnl), Number(b.worst_trade_pnl), profitPct,
          c.followability_hold_score, c.followability_volume_score, c.followability_liq_score, c.followability_final,
          c.consistency_wr_stability, c.consistency_pnl_stability, c.consistency_diversification, c.consistency_final,
          c.pnl_relative_score, c.pnl_profit_factor, c.pnl_pf_score, c.pnl_final,
          c.win_rate_score, c.follow_score,
          c.trades_per_day, c.gross_profit_usd, c.gross_loss_usd,
          c.monthly_wr_cv, c.profitable_months_ratio, c.diversification_ratio, c.p90_benchmark_usd,
          c.has_sufficient_data,
        ]
      );

      // Histórico mensal
      if (period.label==='monthly' && c.has_sufficient_data===1) {
        await execute(
          `INSERT INTO kol_score_history (wallet_address,period,follow_score,followability,consistency,pnl_score,win_rate_score,total_trades)
           VALUES (?,?,?,?,?,?,?,?)`,
          [wallet,'30d',c.follow_score,c.followability_final,c.consistency_final,c.pnl_final,c.win_rate_score,c.total_trades]
        );
      }
    } catch (err) {
      logger.warn(`Metrics error for ${wallet} (${period.label})`, { error: (err as Error).message });
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Atualizar todas as wallets ativas
// ─────────────────────────────────────────────────────────────────────────────

export async function updateAllKolMetrics(): Promise<void> {
  logger.info('Updating KOL metrics for all active wallets...');
  const wallets = await query<{ address: string }>(
    `SELECT DISTINCT w.address FROM wallets w
     JOIN swap_events se ON se.wallet_address=w.address
     WHERE w.is_disqualified=0 AND se.timestamp>=DATE_SUB(NOW(),INTERVAL 30 DAY)
     GROUP BY w.address HAVING COUNT(*)>=1`
  );
  logger.info(`Found ${wallets.length} active wallets to update`);
  for (const { address } of wallets) await updateKolMetrics(address);
  logger.info('KOL metrics update completed');
}

// ─────────────────────────────────────────────────────────────────────────────
// Leaderboard
// ─────────────────────────────────────────────────────────────────────────────

export async function getLeaderboard(
  period = 'monthly', limit = 100, offset = 0, sortBy = 'follow_score'
): Promise<LeaderboardEntry[]> {
  const validSorts: Record<string,string> = {
    follow_score:'km.follow_score', profit_usd:'km.profit_usd',
    win_rate:'km.win_rate', profit_pct:'km.profit_pct',
  };
  const orderCol = validSorts[sortBy] ?? 'km.follow_score';

  const rows = await query<{
    wallet_address: string; label: string|null;
    wins: number; losses: number; total_trades: number;
    profit_usd: number; profit_pct: number; win_rate: number;
    holding_time_avg_s: number; scalping_rate: number; long_trade_rate_pct: number;
    follow_score: number; followability_final: number; consistency_final: number;
    pnl_final: number; win_rate_score: number; trades_per_day: number; diversification_ratio: number;
  }>(
    `SELECT km.wallet_address, w.label,
            km.wins, km.losses, km.total_trades, km.profit_usd,
            COALESCE(km.profit_pct,0) AS profit_pct, km.win_rate,
            COALESCE(km.holding_time_avg_s,0) AS holding_time_avg_s,
            COALESCE(km.scalping_rate,0) AS scalping_rate,
            COALESCE(km.long_trade_rate_pct,0) AS long_trade_rate_pct,
            COALESCE(km.follow_score,0) AS follow_score,
            COALESCE(km.followability_final,0) AS followability_final,
            COALESCE(km.consistency_final,0) AS consistency_final,
            COALESCE(km.pnl_final,0) AS pnl_final,
            COALESCE(km.win_rate_score,0) AS win_rate_score,
            COALESCE(km.trades_per_day,0) AS trades_per_day,
            COALESCE(km.diversification_ratio,0) AS diversification_ratio
     FROM kol_metrics km
     JOIN wallets w ON w.address=km.wallet_address
     WHERE km.period=? AND w.is_disqualified=0 AND km.has_sufficient_data=1
     ORDER BY ${orderCol} DESC
     LIMIT ${limit} OFFSET ${offset}`,
    [period]
  );

  return rows.map((row, idx) => ({
    rank: offset+idx+1,
    wallet_address: row.wallet_address,
    label: row.label,
    wins: Number(row.wins), losses: Number(row.losses), total_trades: Number(row.total_trades),
    profit_usd: Number(row.profit_usd),
    profit_pct: Math.round(Number(row.profit_pct)*100)/100,
    win_rate: Math.round(Number(row.win_rate)*100)/100,
    holding_time_avg_s: Number(row.holding_time_avg_s),
    holding_time_formatted: formatHoldingTime(Number(row.holding_time_avg_s)),
    scalping_rate: Math.round(Number(row.scalping_rate)*100)/100,
    long_trade_rate_pct: Math.round(Number(row.long_trade_rate_pct)*100)/100,
    follow_score: Math.round(Number(row.follow_score)*100)/100,
    follow_score_label: followScoreLabel(Number(row.follow_score)),
    score_components: {
      followability: Math.round(Number(row.followability_final)*100)/100,
      consistency:   Math.round(Number(row.consistency_final)*100)/100,
      pnl:           Math.round(Number(row.pnl_final)*100)/100,
      win_rate:      Math.round(Number(row.win_rate_score)*100)/100,
    },
    trades_per_day: Math.round(Number(row.trades_per_day)*100)/100,
    diversification_ratio: Math.round(Number(row.diversification_ratio)*10000)/10000,
    period,
  }));
}

// ─────────────────────────────────────────────────────────────────────────────
// Detalhes de um KOL
// ─────────────────────────────────────────────────────────────────────────────

export async function getKolDetails(address: string): Promise<KolDetails | null> {
  const walletRows = await query<{
    address: string; label: string|null; first_seen: Date; last_seen: Date; total_transactions: number;
    is_disqualified: number; flag_scalper: number; flag_bundler: number;
    flag_creator_funded: number; flag_sybil: number; scalper_copiability_index: number|null;
  }>(
    `SELECT address,label,first_seen,last_seen,total_transactions,
            COALESCE(is_disqualified,0) AS is_disqualified,
            COALESCE(flag_scalper,0) AS flag_scalper,
            COALESCE(flag_bundler,0) AS flag_bundler,
            COALESCE(flag_creator_funded,0) AS flag_creator_funded,
            COALESCE(flag_sybil,0) AS flag_sybil,
            scalper_copiability_index
     FROM wallets WHERE address=?`,
    [address]
  );
  if (!walletRows.length) return null;
  const w = walletRows[0];

  const metricsRows = await query<{
    period: string; wins: number; losses: number; total_trades: number;
    profit_usd: number; profit_pct: number; win_rate: number;
    holding_time_avg_s: number; scalping_rate: number; long_trade_rate_pct: number;
    follow_score: number; followability_final: number; consistency_final: number;
    pnl_final: number; win_rate_score: number;
    followability_hold_score: number; followability_volume_score: number;
    consistency_wr_stability: number; consistency_pnl_stability: number;
    consistency_diversification: number; pnl_relative_score: number;
    pnl_profit_factor: number; pnl_pf_score: number;
    trades_per_day: number; diversification_ratio: number;
    gross_profit_usd: number; gross_loss_usd: number;
    monthly_wr_cv: number; profitable_months_ratio: number; has_sufficient_data: number;
  }>(
    `SELECT period,wins,losses,total_trades,profit_usd,COALESCE(profit_pct,0) AS profit_pct,
            win_rate,holding_time_avg_s,scalping_rate,long_trade_rate_pct,
            COALESCE(follow_score,0) AS follow_score,
            COALESCE(followability_final,0) AS followability_final,
            COALESCE(consistency_final,0) AS consistency_final,
            COALESCE(pnl_final,0) AS pnl_final,
            COALESCE(win_rate_score,0) AS win_rate_score,
            COALESCE(followability_hold_score,0) AS followability_hold_score,
            COALESCE(followability_volume_score,0) AS followability_volume_score,
            COALESCE(consistency_wr_stability,0) AS consistency_wr_stability,
            COALESCE(consistency_pnl_stability,0) AS consistency_pnl_stability,
            COALESCE(consistency_diversification,0) AS consistency_diversification,
            COALESCE(pnl_relative_score,0) AS pnl_relative_score,
            COALESCE(pnl_profit_factor,0) AS pnl_profit_factor,
            COALESCE(pnl_pf_score,0) AS pnl_pf_score,
            COALESCE(trades_per_day,0) AS trades_per_day,
            COALESCE(diversification_ratio,0) AS diversification_ratio,
            COALESCE(gross_profit_usd,0) AS gross_profit_usd,
            COALESCE(gross_loss_usd,0) AS gross_loss_usd,
            COALESCE(monthly_wr_cv,0) AS monthly_wr_cv,
            COALESCE(profitable_months_ratio,0) AS profitable_months_ratio,
            COALESCE(has_sufficient_data,0) AS has_sufficient_data
     FROM kol_metrics WHERE wallet_address=?
     ORDER BY FIELD(period,'daily','weekly','monthly','all_time')`,
    [address]
  );

  const swapRows = await query<{
    tx_hash: string; timestamp: Date; dex_name: string;
    token_in_symbol: string; token_out_symbol: string;
    token_in_address: string; token_out_address: string;
    value_usd: number; pnl: number; is_win: number;
    holding_time_s: number|null; is_long_trade: number|null;
    pnl_base: number|null; pnl_base_symbol: string|null; swap_type: string|null;
  }>(
    `SELECT tx_hash,timestamp,dex_name,token_in_symbol,token_out_symbol,
            token_in_address,token_out_address,value_usd,pnl,is_win,
            holding_time_s,is_long_trade,
            pnl_base,pnl_base_symbol,swap_type
     FROM swap_events WHERE wallet_address=? ORDER BY timestamp DESC LIMIT 50`,
    [address]
  );

  const disqReason = w.flag_scalper ? 'SCALPER' : w.flag_bundler ? 'BUNDLER' : w.flag_creator_funded ? 'CREATOR_FUNDED' : w.flag_sybil ? 'SYBIL' : null;
  const monthly = metricsRows.find((m) => m.period==='monthly');
  const fs = Number(monthly?.follow_score)||0;

  return {
    wallet_address: w.address, label: w.label,
    first_seen: w.first_seen, last_seen: w.last_seen, total_transactions: w.total_transactions,
    is_disqualified: w.is_disqualified===1, disqualification_reason: disqReason,
    flags: { scalper: w.flag_scalper===1, bundler: w.flag_bundler===1, creator_funded: w.flag_creator_funded===1, sybil: w.flag_sybil===1 },
    copiability_index: w.scalper_copiability_index!==null ? Number(w.scalper_copiability_index) : null,
    copiability_pct: w.scalper_copiability_index!==null ? Math.round(Number(w.scalper_copiability_index)*10000)/100 : null,
    holding_analysis: {
      scalping_threshold_s: config.indexer.scalpingThresholdSeconds,
      scalping_threshold_formatted: formatHoldingTime(config.indexer.scalpingThresholdSeconds),
      avg_holding_s: Number(monthly?.holding_time_avg_s)||null,
      avg_holding_formatted: formatHoldingTime(Number(monthly?.holding_time_avg_s)||null),
      long_trade_rate_pct: Number(monthly?.long_trade_rate_pct)||null,
      scalping_rate_pct: Number(monthly?.scalping_rate)||0,
      follow_score: fs, follow_score_label: followScoreLabel(fs),
    },
    metrics_by_period: metricsRows.map((m) => ({
      period: m.period, wins: Number(m.wins), losses: Number(m.losses), total_trades: Number(m.total_trades),
      profit_usd: Number(m.profit_usd), profit_pct: Math.round(Number(m.profit_pct)*100)/100,
      win_rate: Math.round(Number(m.win_rate)*100)/100,
      follow_score: Math.round(Number(m.follow_score)*100)/100,
      follow_score_label: followScoreLabel(Number(m.follow_score)),
      has_sufficient_data: m.has_sufficient_data===1,
      score_components: {
        followability: { final: Math.round(Number(m.followability_final)*100)/100, hold_score: Math.round(Number(m.followability_hold_score)*100)/100, volume_score: Math.round(Number(m.followability_volume_score)*100)/100, liq_score: 75 },
        consistency: { final: Math.round(Number(m.consistency_final)*100)/100, wr_stability: Math.round(Number(m.consistency_wr_stability)*100)/100, pnl_stability: Math.round(Number(m.consistency_pnl_stability)*100)/100, diversification: Math.round(Number(m.consistency_diversification)*100)/100 },
        pnl: { final: Math.round(Number(m.pnl_final)*100)/100, relative_score: Math.round(Number(m.pnl_relative_score)*100)/100, profit_factor: Math.round(Number(m.pnl_profit_factor)*100)/100, pf_score: Math.round(Number(m.pnl_pf_score)*100)/100 },
        win_rate: { score: Math.round(Number(m.win_rate_score)*100)/100, raw_pct: Math.round(Number(m.win_rate)*100)/100 },
      },
      aux_metrics: { trades_per_day: Math.round(Number(m.trades_per_day)*100)/100, diversification_ratio: Math.round(Number(m.diversification_ratio)*10000)/10000, gross_profit_usd: Number(m.gross_profit_usd), gross_loss_usd: Number(m.gross_loss_usd), monthly_wr_cv: Math.round(Number(m.monthly_wr_cv)*100)/100, profitable_months_ratio: Math.round(Number(m.profitable_months_ratio)*100)/100 },
    })),
    recent_swaps: swapRows.map((s) => ({
      tx_hash: s.tx_hash, timestamp: s.timestamp, dex_name: s.dex_name,
      token_in_symbol: s.token_in_symbol, token_out_symbol: s.token_out_symbol,
      token_in_address: s.token_in_address, token_out_address: s.token_out_address,
      value_usd: Number(s.value_usd), pnl: Number(s.pnl), is_win: s.is_win===1,
      holding_time_s: s.holding_time_s!==null ? Number(s.holding_time_s) : null,
      holding_time_formatted: formatHoldingTime(s.holding_time_s!==null ? Number(s.holding_time_s) : null),
      is_long_trade: s.is_long_trade,
      // PnL na moeda base (ex: WETH) — disponível apenas em vendas (swap_type='sell')
      pnl_base: s.pnl_base !== null ? Number(s.pnl_base) : null,
      pnl_base_symbol: s.pnl_base_symbol ?? null,
      swap_type: s.swap_type ?? null,
    })),
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Estatísticas da plataforma
// ─────────────────────────────────────────────────────────────────────────────

export async function getPlatformStats(): Promise<Record<string, unknown>> {
  const statsRows = await query<{
    total_wallets: number; active_wallets: number; disqualified_wallets: number;
    total_swaps: number; total_volume_usd: number;
  }>(
    `SELECT COUNT(DISTINCT w.address) AS total_wallets,
            COUNT(DISTINCT CASE WHEN COALESCE(w.is_disqualified,0)=0 THEN w.address END) AS active_wallets,
            COUNT(DISTINCT CASE WHEN COALESCE(w.is_disqualified,0)=1 THEN w.address END) AS disqualified_wallets,
            COUNT(se.id) AS total_swaps, COALESCE(SUM(se.value_usd),0) AS total_volume_usd
     FROM wallets w LEFT JOIN swap_events se ON se.wallet_address=w.address`
  );
  const topTokenRows = await query<{ symbol: string; trade_count: number; volume_usd: number }>(
    `SELECT token_out_symbol AS symbol, COUNT(*) AS trade_count, SUM(value_usd) AS volume_usd
     FROM swap_events WHERE timestamp>=DATE_SUB(NOW(),INTERVAL 24 HOUR) AND token_out_symbol IS NOT NULL
     GROUP BY token_out_symbol ORDER BY trade_count DESC LIMIT 10`
  );
  return {
    total_wallets: Number(statsRows[0]?.total_wallets)||0,
    active_wallets: Number(statsRows[0]?.active_wallets)||0,
    disqualified_wallets: Number(statsRows[0]?.disqualified_wallets)||0,
    total_swaps: Number(statsRows[0]?.total_swaps)||0,
    total_volume_usd: Number(statsRows[0]?.total_volume_usd)||0,
    top_tokens_24h: topTokenRows.map((t) => ({ symbol: t.symbol, trade_count: Number(t.trade_count), volume_usd: Number(t.volume_usd) })),
  };
}
