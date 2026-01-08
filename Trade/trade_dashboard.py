# -*- coding: utf-8 -*-
"""
Professional Trade Dashboard (Integrated)
Combines strict calculation logic from update_trades.py
and specific visual styling/stats from trade_app.py
"""

import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
import plotly.io as pio
import os
import time
import numpy as np
from datetime import datetime

# ==========================================
# 1. Configuration
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "swing_trades.csv")

# Output Filename with Timestamp
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_HTML = os.path.join(BASE_DIR, f"trade_record_{timestamp_str}.html")

# Capital Settings (USD Base)
TOTAL_CAPITAL_USD = 1_000_000


# ==========================================
# 2. Market Data Update Logic
# ==========================================
def update_active_trades():
    print("--- Starting Market Data Update ---")
    if not os.path.exists(CSV_FILE):
        print("CSV file not found.")
        return

    df = pd.read_csv(CSV_FILE)

    if 'Status' not in df.columns:
        df['Status'] = 'Open'

    active_mask = df['Status'].str.upper() == 'OPEN'
    tickers = df.loc[active_mask, 'Ticker'].unique().tolist()

    if not tickers:
        print("No active trades to update.")
    else:
        print(f"Fetching data for: {tickers}")

        yf_mapping = {}
        yf_tickers = []
        for t in tickers:
            # Handle special tickers
            if t == "USDJPY":
                yf_sym = "USDJPY=X"
            elif t == "SI=F":
                yf_sym = "SI=F"
            elif t == "NQ=F":
                yf_sym = "NQ=F"
            elif t == "CL=F":
                yf_sym = "CL=F"
            else:
                yf_sym = t.strip()

            yf_tickers.append(yf_sym)
            yf_mapping[t] = yf_sym

        try:
            # Download Data
            data = yf.download(yf_tickers, period="5d", group_by='ticker', progress=False, threads=True)

            if data.empty:
                print("❌ Download returned no data.")
            else:
                for index, row in df.iterrows():
                    if str(row['Status']).upper() != 'OPEN':
                        continue

                    ticker = row['Ticker']
                    yf_sym = yf_mapping.get(ticker, ticker)

                    try:
                        last_price = None

                        if isinstance(data.columns, pd.MultiIndex):
                            if yf_sym in data.columns.levels[0]:
                                close_series = data[yf_sym]['Close']
                                last_valid_idx = close_series.last_valid_index()
                                if last_valid_idx:
                                    last_price = close_series.loc[last_valid_idx]
                        else:
                            if len(yf_tickers) == 1 or yf_sym in yf_tickers:
                                if 'Close' in data.columns:
                                    last_valid_idx = data['Close'].last_valid_index()
                                    if last_valid_idx:
                                        last_price = data.loc[last_valid_idx, 'Close']

                        if last_price is not None:
                            last_price = float(last_price)

                            # 1. Update LastPrice
                            df.at[index, 'LastPrice'] = round(last_price, 4)

                            # 2. Define Multiplier
                            multiplier = 1
                            if ticker == "USDJPY":
                                multiplier = 1000
                            elif ticker == "SI=F":
                                multiplier = 1
                            elif ticker == "NQ=F":
                                multiplier = 20
                            elif ticker == "ES=F":
                                multiplier = 50

                            # 3. Calculate PnL
                            entry = float(row['EntryPrice'])
                            qty = float(row['Quantity'])
                            is_long = str(row['Direction']).upper() == 'LONG'
                            diff = (last_price - entry) if is_long else (entry - last_price)
                            pnl = diff * qty * multiplier

                            df.at[index, 'PnLUSD'] = round(pnl, 2)

                            # 4. Update Market Value (Exposure)
                            # Store pure magnitude here, direction is handled by 'Direction' column
                            df.at[index, 'USDNotional'] = round(last_price * qty * multiplier, 2)

                            print(f"✅ {ticker}: Last={last_price:.2f}, PnL={pnl:.0f}")
                        else:
                            print(f"❌ Failed to extract price for {ticker}")

                    except Exception as e:
                        print(f"Error processing row {index} ({ticker}): {e}")

        except Exception as e:
            print(f"Global Error in download: {e}")

    df.to_csv(CSV_FILE, index=False)
    print("--- CSV Update Complete ---\n")


# ==========================================
# 3. Data Loading & Stats
# ==========================================
def load_and_process_data():
    if not os.path.exists(CSV_FILE):
        return pd.DataFrame(), pd.DataFrame(), 0, 0, 0, 0, {}

    df = pd.read_csv(CSV_FILE)

    required_cols = ['Ticker', 'Direction', 'Status', 'USDNotional', 'PnLUSD', 'EntryDate', 'EntryPrice', 'Quantity',
                     'Notes', 'LastPrice']
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0 if col in ['USDNotional', 'PnLUSD', 'EntryPrice', 'Quantity', 'LastPrice'] else ""

    active_df = df[df['Status'].str.upper() == 'OPEN'].copy()
    closed_df = df[df['Status'].str.upper() == 'CLOSED'].copy()

    # --- Active Stats ---
    active_df['Weight %'] = (active_df['USDNotional'] / TOTAL_CAPITAL_USD) * 100
    gross_exposure = active_df['USDNotional'].abs().sum()

    # Net Exposure Logic
    active_df['SignedNotional'] = active_df.apply(
        lambda x: -abs(x['USDNotional']) if str(x['Direction']).upper() == 'SHORT' else abs(x['USDNotional']),
        axis=1
    )
    net_exposure = active_df['SignedNotional'].sum()

    total_pnl = active_df['PnLUSD'].sum()
    cash_balance = TOTAL_CAPITAL_USD - gross_exposure

    # --- Historical Stats ---
    stats = calculate_performance_stats(closed_df)

    return active_df, closed_df, gross_exposure, net_exposure, total_pnl, cash_balance, stats


def calculate_performance_stats(df):
    if df.empty:
        return {'WinRate': 0, 'ProfitFactor': 0, 'AvgWin': 0, 'AvgLoss': 0, 'TradeCount': 0, 'TotalRealized': 0,
                'Expectancy': 0}

    pnl = df['PnLUSD']
    winners = pnl[pnl > 0]
    losers = pnl[pnl <= 0]

    count = len(pnl)
    win_count = len(winners)

    win_rate = (win_count / count) * 100 if count > 0 else 0
    total_realized = pnl.sum()

    gross_profit = winners.sum()
    gross_loss = abs(losers.sum())

    pf = (gross_profit / gross_loss) if gross_loss > 0 else float('inf')
    avg_win = winners.mean() if win_count > 0 else 0
    avg_loss = losers.mean() if len(losers) > 0 else 0
    expectancy = total_realized / count if count > 0 else 0

    return {
        'WinRate': win_rate,
        'ProfitFactor': pf,
        'AvgWin': avg_win,
        'AvgLoss': avg_loss,
        'TradeCount': count,
        'TotalRealized': total_realized,
        'Expectancy': expectancy
    }


# ==========================================
# 4. Chart Generation (ONE CHART / ABSOLUTE OVERLAY)
# ==========================================
def generate_allocation_chart(df):
    if df.empty:
        return "<div style='text-align:center; padding:20px; color:#64748b;'>No Active Positions</div>"

    # Sort by Absolute Exposure Magnitude (Largest positions on top/bottom)
    df_sorted = df.sort_values(by='USDNotional', ascending=True)

    tickers = df_sorted['Ticker'].tolist()

    # 1. Prepare Data for Absolute Magnitude Plotting
    # We plot everything as positive bars so they "stack/overlay" nicely
    abs_exposures = df_sorted['USDNotional'].abs().tolist()
    abs_pnls = df_sorted['PnLUSD'].abs().tolist()

    # 2. Real Values for Hover Text
    real_exposures = df_sorted['SignedNotional'].tolist()
    real_pnls = df_sorted['PnLUSD'].tolist()

    # 3. Colors
    # Exposure: Blue for Long, Purple for Short (Purple implies Short without meaning 'Loss')
    colors_exp = ['#3b82f6' if x >= 0 else '#a855f7' for x in real_exposures]

    # PnL: Green for Profit, Red for Loss
    colors_pnl = ['#10B981' if x >= 0 else '#F43F5E' for x in real_pnls]

    fig = go.Figure()

    # Trace 1: Exposure (The "Container" Bar) - Plotted First
    fig.add_trace(go.Bar(
        y=tickers,
        x=abs_exposures,
        orientation='h',
        name='Position Size',
        marker=dict(color=colors_exp, line=dict(width=0)),
        # Custom Hover to show real signed values
        hovertemplate='<b>%{y}</b><br>Size: %{customdata:$,.0f}<extra></extra>',
        customdata=real_exposures
    ))

    # Trace 2: PnL (The "Inner" Bar) - Plotted Second (On Top)
    fig.add_trace(go.Bar(
        y=tickers,
        x=abs_pnls,
        orientation='h',
        name='PnL',
        marker=dict(color=colors_pnl, line=dict(width=0)),
        # Custom Hover to show real signed PnL
        hovertemplate='<b>%{y}</b><br>PnL: %{customdata:+$,.0f}<extra></extra>',
        customdata=real_pnls,
        # Narrower bar to make it look like it's "inside"
        width=0.5
    ))

    chart_height = max(350, len(tickers) * 45)

    fig.update_layout(
        title="Portfolio Exposure (Bar) vs PnL (Inner)",
        title_font=dict(size=14, color="#e2e8f0"),
        barmode='overlay',  # Key: Allows bars to sit on top of each other
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, t=40, b=0),
        height=chart_height,
        showlegend=False,
        font=dict(family="Inter, sans-serif", color="#94a3b8"),
        xaxis=dict(
            showgrid=True,
            gridcolor='rgba(255,255,255,0.1)',
            zeroline=False,
            tickprefix="$",
            title="Value (Absolute Magnitude)"
        ),
        yaxis=dict(showgrid=False, tickfont=dict(color="#e2e8f0", size=13, weight="bold"))
    )

    return pio.to_html(fig, full_html=False, include_plotlyjs='cdn')


# ==========================================
# 5. HTML Templates
# ==========================================
HTML_HEAD = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Roboto+Mono:wght@400;500;700&display=swap');
    :root { --bg-color: #0b0e11; --panel-bg: #151a21; --border-color: #2a2e39; --text-main: #e2e8f0; --text-muted: #64748b; --accent: #3b82f6; --profit: #10B981; --loss: #F43F5E; --warning: #F59E0B; }
    body { font-family: 'Inter', sans-serif; background-color: var(--bg-color); color: var(--text-main); margin: 0; padding: 20px; font-size: 14px; }
    .container { max-width: 1400px; margin: 0 auto; }
    .header-flex { display: flex; justify-content: space-between; align-items: flex-end; margin-bottom: 20px; border-bottom: 1px solid var(--border-color); padding-bottom: 15px; }
    .metrics-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 25px; }
    .stats-row { display: grid; grid-template-columns: repeat(5, 1fr); gap: 10px; margin-bottom: 25px; }
    .metric-box { background: var(--panel-bg); padding: 15px; border: 1px solid var(--border-color); border-radius: 4px; }
    .metric-label { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; font-weight: 600; }
    .metric-val { font-family: 'Roboto Mono', monospace; font-size: 1.4rem; font-weight: 600; margin-top: 5px; color: #fff; }
    .stat-card { background: rgba(255,255,255,0.02); padding: 10px; border-radius: 4px; border: 1px solid var(--border-color); text-align:center; }
    .stat-title { font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase; }
    .stat-num { font-family: 'Roboto Mono'; font-size: 1.1rem; font-weight: 700; margin-top: 3px; }
    .chart-container { background: var(--panel-bg); border: 1px solid var(--border-color); border-radius: 4px; padding: 15px; margin-bottom: 25px; }
    .tab-nav { display: flex; gap: 20px; margin-bottom: 15px; border-bottom: 1px solid var(--border-color); }
    .tab-btn { background: none; border: none; padding: 10px 0; color: var(--text-muted); font-size: 0.95rem; cursor: pointer; border-bottom: 2px solid transparent; }
    .tab-btn.active { color: var(--accent); border-bottom: 2px solid var(--accent); font-weight: 600; }
    .table-responsive { overflow-x: auto; background: var(--panel-bg); border: 1px solid var(--border-color); border-radius: 4px; }
    table { width: 100%; border-collapse: collapse; white-space: nowrap; }
    th { text-align: right; padding: 10px 16px; background: #1b2129; color: var(--text-muted); font-size: 0.75rem; text-transform: uppercase; border-bottom: 1px solid var(--border-color); }
    th:first-child { text-align: left; }
    td { padding: 10px 16px; border-bottom: 1px solid var(--border-color); font-family: 'Roboto Mono', monospace; font-size: 0.9rem; text-align: right; color: #cbd5e1; }
    td:first-child { text-align: left; font-family: 'Inter', sans-serif; font-weight: 600; color: #fff; }
    .text-profit { color: var(--profit); } .text-loss { color: var(--loss); }
    .badge { padding: 2px 6px; border-radius: 3px; font-size: 0.7rem; margin-left: 5px; border: 1px solid; }
    .badge-long { color: var(--profit); border-color: rgba(16, 185, 129, 0.3); background: rgba(16, 185, 129, 0.1); }
    .badge-short { color: var(--loss); border-color: rgba(244, 63, 94, 0.3); background: rgba(244, 63, 94, 0.1); }
    .risk-alert { color: var(--warning); font-size: 0.8rem; margin-left: 5px; }
    .hidden { display: none; }
    .section-title { font-size: 0.9rem; font-weight: 700; color: #fff; margin-bottom: 10px; border-left: 3px solid var(--accent); padding-left: 10px; }
</style>
</head>
<body>
"""

HTML_FOOTER = """
<script>
    function openTab(tabId) {
        document.querySelectorAll('.tab-content').forEach(d => d.classList.add('hidden'));
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.getElementById(tabId).classList.remove('hidden');
        document.querySelector(`button[onclick="openTab('${tabId}')"]`).classList.add('active');
    }
</script>
</body>
</html>
"""


# ==========================================
# 6. Report Generation
# ==========================================
def generate_report():
    print(f"--- Generating Trade Report: {OUTPUT_HTML} ---")
    active_df, closed_df, gross_exp, net_exp, total_pnl, cash, stats = load_and_process_data()
    nav = TOTAL_CAPITAL_USD + total_pnl

    html = HTML_HEAD
    html += f"""
    <div class="container">
        <div class="header-flex">
            <div>
                <h2 style="margin:0; font-weight:700;">My Trade Journal</h2>
                <div style="color:#64748b; font-size:0.85rem;">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</div>
            </div>
        </div>

        <div class="metrics-row">
            <div class="metric-box">
                <div class="metric-label">NAV (Net Liquidation)</div>
                <div class="metric-val">${nav:,.0f}</div>
            </div>
            <div class="metric-box">
                <div class="metric-label">Floating P&L</div>
                <div class="metric-val {'text-profit' if total_pnl >= 0 else 'text-loss'}">{total_pnl:+,.0f}</div>
            </div>
            <div class="metric-box">
                <div class="metric-label">Net Exposure</div>
                <div class="metric-val" style="color: {'#3b82f6' if net_exp >= 0 else '#ef4444'}">${net_exp:+,.0f}</div>
                <div style="font-size:0.75rem; color:#64748b;">Gross: ${gross_exp:,.0f}</div>
            </div>
            <div class="metric-box">
                <div class="metric-label">Buying Power</div>
                <div class="metric-val" style="color:#10B981">${cash:,.0f}</div>
            </div>
        </div>

        <div class="section-title">PERFORMANCE ANALYTICS (CLOSED TRADES)</div>
        <div class="stats-row">
            <div class="stat-card">
                <div class="stat-title">Win Rate</div>
                <div class="stat-num" style="color:{'#10B981' if stats['WinRate'] > 50 else '#F43F5E'}">{stats['WinRate']:.1f}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-title">Profit Factor</div>
                <div class="stat-num">{stats['ProfitFactor']:.2f}</div>
            </div>
            <div class="stat-card">
                <div class="stat-title">Expectancy</div>
                <div class="stat-num">${stats['Expectancy']:,.0f}</div>
            </div>
            <div class="stat-card">
                <div class="stat-title">Avg Win</div>
                <div class="stat-num text-profit">+${stats['AvgWin']:,.0f}</div>
            </div>
            <div class="stat-card">
                <div class="stat-title">Avg Loss</div>
                <div class="stat-num text-loss">${stats['AvgLoss']:,.0f}</div>
            </div>
        </div>

        <div class="chart-container">{generate_allocation_chart(active_df)}</div>

        <div class="tab-nav">
            <button class="tab-btn active" onclick="openTab('active-trades')">Active Positions ({len(active_df)})</button>
            <button class="tab-btn" onclick="openTab('past-trades')">Trade History ({len(closed_df)})</button>
        </div>

        <div id="active-trades" class="tab-content">
            <div class="table-responsive">
                <table>
                    <thead>
                        <tr><th>Ticker</th><th>Entry</th><th>Last</th><th>Qty</th><th>Net Notional</th><th>Weight</th><th>Unrealized P&L</th><th>Note</th></tr>
                    </thead>
                    <tbody>
    """

    if active_df.empty:
        html += '<tr><td colspan="8" style="text-align:center; padding:30px;">No active positions.</td></tr>'
    else:
        for _, row in active_df.iterrows():
            is_long = str(row['Direction']).upper() == 'LONG'
            badge = "badge-long" if is_long else "badge-short"
            pnl_class = "text-profit" if row['PnLUSD'] >= 0 else "text-loss"
            signed_notional = row['SignedNotional']
            weight = row['Weight %']
            risk_alert = '<span class="risk-alert">⚠️ High Conc.</span>' if weight > 20 else ""
            notional_str = f"${signed_notional:,.0f}" if is_long else f"-${abs(signed_notional):,.0f}"

            html += f"""
            <tr>
                <td>{row['Ticker']} <span class="badge {badge}">{row['Direction'][0]}</span></td>
                <td>{row['EntryDate']} @ {row['EntryPrice']}</td>
                <td style="color:#fff;">{row['LastPrice']}</td>
                <td>{row['Quantity']:,}</td>
                <td style="color:{'#e2e8f0' if is_long else '#ef4444'}">{notional_str}</td>
                <td>{weight:.1f}% {risk_alert}</td>
                <td class="{pnl_class}">${row['PnLUSD']:+,.0f}</td>
                <td style="text-align:left; color:#64748b; font-size:0.8rem;">{row['Notes']}</td>
            </tr>"""

    html += """</tbody></table></div></div>"""

    html += """
        <div id="past-trades" class="tab-content hidden">
            <div class="table-responsive">
                <table>
                    <thead><tr><th>Ticker</th><th>Entry</th><th>Exit Price</th><th>Realized P&L</th><th>Note</th></tr></thead>
                    <tbody>
    """

    if closed_df.empty:
        html += '<tr><td colspan="5" style="text-align:center; padding:30px;">No closed trades history found.</td></tr>'
    else:
        for _, row in closed_df.iterrows():
            pnl_class = "text-profit" if row['PnLUSD'] >= 0 else "text-loss"
            html += f"""
            <tr>
                <td>{row['Ticker']}</td>
                <td>{row['EntryDate']}</td>
                <td>{row['LastPrice']}</td>
                <td class="{pnl_class}">${row['PnLUSD']:+,.0f}</td>
                <td style="text-align:left; color:#64748b;">{row['Notes']}</td>
            </tr>"""

    html += """</tbody></table></div></div></div>"""
    html += HTML_FOOTER

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[Success] Report generated: {OUTPUT_HTML}")


# ==========================================
# 7. Main Execution
# ==========================================
if __name__ == "__main__":
    update_active_trades()
    generate_report()