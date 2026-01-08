import pandas as pd
import json
import numpy as np
import datetime
import os
import glob

# ==========================================
# Configuration
# ==========================================
FILE_PREFIX = "hk_option_raw_data_"
TH_VOL_OI_WATCH = 1.0  # Yellow
TH_VOL_OI_ALERT = 3.0  # Red
TH_TURNOVER_SEPARATOR = 1000000  # 1 Million HKD


# ==========================================
# Custom JSON Encoder
# ==========================================
class RobustEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, int)):
            return int(obj)
        if isinstance(obj, (np.floating, float)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.strftime("%Y-%m-%d")
        if pd.isna(obj):
            return None
        return super(RobustEncoder, self).default(obj)


def find_latest_csv():
    files = glob.glob(f"{FILE_PREFIX}*.csv")
    if not files:
        files = glob.glob(f"{FILE_PREFIX}*.xlsx")
    if not files:
        return None
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]


def analyze_stock_data(df, ul_price, stock_code):
    # Standardize
    df['option_type'] = df['option_type'].str.upper().str.strip()
    calls = df[df['option_type'] == 'CALL'].copy()
    puts = df[df['option_type'] == 'PUT'].copy()

    # --- Walls (Modified: Only +/- 10 strikes around spot) ---
    call_oi_map = calls.groupby('Strike')['OpenInterest'].sum()
    put_oi_map = puts.groupby('Strike')['OpenInterest'].sum()

    # Get all unique strikes sorted
    all_strikes = sorted(df['Strike'].unique())

    if all_strikes:
        # Find the strike closest to spot price
        closest_strike = min(all_strikes, key=lambda x: abs(x - ul_price))
        try:
            mid_idx = all_strikes.index(closest_strike)
        except ValueError:
            mid_idx = 0

        # Define window: 10 strikes left, 10 strikes right
        start_idx = max(0, mid_idx - 10)
        end_idx = min(len(all_strikes), mid_idx + 11)  # +11 to include the 10th item on the right

        target_strikes = all_strikes[start_idx:end_idx]

        # Filter the OI maps to only these strikes
        filtered_call_oi = call_oi_map[call_oi_map.index.isin(target_strikes)]
        filtered_put_oi = put_oi_map[put_oi_map.index.isin(target_strikes)]

        # Find walls within the filtered range
        call_wall = filtered_call_oi.idxmax() if not filtered_call_oi.empty else 0
        put_wall = filtered_put_oi.idxmax() if not filtered_put_oi.empty else 0
    else:
        call_wall = 0
        put_wall = 0

    # --- Hotspots (Max Turnover) ---
    max_to_row = df.loc[df['Turnover'].idxmax()] if not df.empty else None
    hotspot_text = "None"
    if max_to_row is not None:
        hotspot_text = f"{max_to_row['option_type']} ${max_to_row['Strike']} ({max_to_row['Expiry']})"

    # --- Net Delta ---
    calls['Delta'] = pd.to_numeric(calls['Delta'], errors='coerce').fillna(0)
    puts['Delta'] = pd.to_numeric(puts['Delta'], errors='coerce').fillna(0)
    net_delta = (calls['Delta'] * calls['OpenInterest']).sum() + (puts['Delta'] * puts['OpenInterest']).sum()

    # --- Flow Analysis ---
    call_turnover = calls['Turnover'].sum()
    put_turnover = puts['Turnover'].sum()
    total_turnover = call_turnover + put_turnover
    call_pct = (call_turnover / total_turnover * 100) if total_turnover > 0 else 0

    # --- IV & Expected Move ---
    expiry_oi = df.groupby('Expiry')['OpenInterest'].sum()
    dominant_expiry = expiry_oi.idxmax() if not expiry_oi.empty else "N/A"

    monthly_df = df[df['Expiry'] == dominant_expiry].copy() if not expiry_oi.empty else df.copy()

    avg_iv = 0
    all_strikes = monthly_df['Strike'].unique()
    if len(all_strikes) > 0:
        atm_strike = min(all_strikes, key=lambda x: abs(x - ul_price))
        atm_opts = monthly_df[monthly_df['Strike'] == atm_strike]
        iv_values = [iv for iv in atm_opts['IV'].tolist() if iv > 0]
        if iv_values: avg_iv = sum(iv_values) / len(iv_values)

    iv_calc = avg_iv / 100.0 if avg_iv > 5 else avg_iv
    expected_daily_move = ul_price * (iv_calc / 16.0)

    # --- IV Skew ---
    m_calls = monthly_df[monthly_df['option_type'] == 'CALL']
    m_puts = monthly_df[monthly_df['option_type'] == 'PUT']
    otm_calls = m_calls[m_calls['Strike'] > ul_price * 1.02]
    otm_puts = m_puts[m_puts['Strike'] < ul_price * 0.98]
    avg_call_iv = otm_calls['IV'].mean() if not otm_calls.empty else avg_iv
    avg_put_iv = otm_puts['IV'].mean() if not otm_puts.empty else avg_iv
    iv_skew = avg_put_iv - avg_call_iv

    # --- Movers Processing ---
    active = df[df['volume'] > 0].copy()

    def calc_ratio(row):
        vol = row['volume']
        oi = row['OpenInterest']
        if oi <= 0: return 2.0
        return round(vol / oi, 2)

    active['vol_oi_ratio'] = active.apply(calc_ratio, axis=1)
    sorted_active = active.sort_values(by='Turnover', ascending=False)

    # Single Stock View List
    top_movers_df = sorted_active.head(50)
    movers_list = []
    separator_added = False
    for idx, row in top_movers_df.iterrows():
        if not separator_added and row['Turnover'] < TH_TURNOVER_SEPARATOR:
            movers_list.append({"_is_separator": True})
            separator_added = True
        r_dict = row[
            ['code', 'option_type', 'Strike', 'Expiry', 'Price', 'volume', 'OpenInterest', 'Turnover', 'vol_oi_ratio',
             'IV']].to_dict()
        movers_list.append(r_dict)

    # Global Scanner List
    global_candidates = active[
        (active['vol_oi_ratio'] >= TH_VOL_OI_WATCH) &
        (active['Turnover'] >= TH_TURNOVER_SEPARATOR)
        ].copy()
    global_candidates['stock_code'] = stock_code

    stock_agg = {
        "stock": stock_code,
        "price": ul_price,
        "total_turnover": total_turnover,
        "call_pct": call_pct,
        "put_pct": 100 - call_pct,
        "net_delta": net_delta,
        "atm_iv": avg_iv
    }

    return {
        "summary": {
            "spot": ul_price,
            "call_wall": call_wall,
            "put_wall": put_wall,
            "hotspot": hotspot_text,
            "net_delta": net_delta,
            "atm_iv": round(avg_iv, 2),
            "dominant_expiry": dominant_expiry,
            "exp_move": round(expected_daily_move, 2),
            "iv_skew": round(iv_skew, 2)
        },
        "movers": movers_list,
        "global_movers": global_candidates[
            ['stock_code', 'code', 'option_type', 'Strike', 'Expiry', 'Price', 'volume', 'OpenInterest', 'Turnover',
             'vol_oi_ratio', 'IV']].to_dict(orient='records'),
        "stock_agg": stock_agg
    }


def parse_csv_to_data(file_path):
    print(f"📂 Loading data from: {file_path}...")
    if file_path.endswith('.xlsx'):
        df = pd.read_excel(file_path)
    else:
        df = pd.read_csv(file_path)

    df.columns = [c.strip() for c in df.columns]
    rename_map = {
        'turnover': 'Turnover', 'Turnover': 'Turnover',
        'OpenInterest': 'OpenInterest', 'ul_price': 'ul_price',
        'stock_owner': 'stock_owner'
    }
    df.rename(columns=rename_map, inplace=True)

    df['Turnover'] = pd.to_numeric(df['Turnover'], errors='coerce').fillna(0)
    df['OpenInterest'] = pd.to_numeric(df['OpenInterest'], errors='coerce').fillna(0)
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)

    stocks = df['stock_owner'].unique()

    single_stock_data = {}
    master_scanner_list = []
    stock_ranking_list = []

    for stock in stocks:
        print(f"   -> Processing {stock}...")
        sub_df = df[df['stock_owner'] == stock].copy()
        ul_price = sub_df['ul_price'].max()

        # Heatmap Data
        pivot_turnover = sub_df.pivot_table(index='Expiry', columns='Strike', values='Turnover', aggfunc='sum').fillna(
            0)
        pivot_oi = sub_df.pivot_table(index='Expiry', columns='Strike', values='OpenInterest', aggfunc='sum').fillna(0)
        pivot_ratio = pivot_turnover.div(pivot_oi.replace(0, np.nan)).fillna(0).round(2)

        def df_to_dict(dframe):
            if dframe.empty: return {"index": [], "columns": [], "data": []}
            dframe.sort_index(inplace=True)
            dframe.columns = dframe.columns.astype(float)
            dframe = dframe.sort_index(axis=1)
            return {"index": dframe.index.astype(str).tolist(), "columns": dframe.columns.tolist(),
                    "data": dframe.values.tolist()}

        results = analyze_stock_data(sub_df, ul_price, stock)

        master_scanner_list.extend(results['global_movers'])
        stock_ranking_list.append(results['stock_agg'])

        single_stock_data[stock] = {
            "today": df_to_dict(pivot_turnover),
            "oi": df_to_dict(pivot_oi),
            "ratio": df_to_dict(pivot_ratio),
            "analysis": {
                "summary": results['summary'],
                "movers": results['movers']
            },
            "params": {"ul_price": ul_price}
        }

    master_scanner_list.sort(key=lambda x: x['Turnover'], reverse=True)
    stock_ranking_list.sort(key=lambda x: x['total_turnover'], reverse=True)

    return {
        "stocks": single_stock_data,
        "market_scanner": master_scanner_list,
        "stock_ranking": stock_ranking_list
    }


def generate_html(full_data, source_filename):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"HK_Option_Market_Analysis_v6_{timestamp}.html"
    print(f"📝 Generating HTML: {output_file}...")

    try:
        json_data = json.dumps(full_data, cls=RobustEncoder)
    except Exception as e:
        print(f"❌ JSON Error: {e}")
        return

    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HK Option Master v6</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        :root {{ --bg-dark: #f4f6f9; --card-shadow: 0 4px 6px rgba(0,0,0,0.05); }}
        body {{ background-color: var(--bg-dark); padding: 20px; height: 100vh; display: flex; flex-direction: column; overflow: hidden; font-family: 'Segoe UI', sans-serif; }}

        .controls-area {{ flex: 0 0 auto; margin-bottom: 10px; background: #fff; padding: 15px; border-radius: 8px; box-shadow: var(--card-shadow); }}
        .content-wrapper {{ flex: 1 1 auto; position: relative; overflow: hidden; background: white; border-radius: 8px; box-shadow: var(--card-shadow); margin-top: 10px; display: flex; flex-direction: column; }}
        .scroll-container {{ flex: 1; overflow: auto; padding: 0; position: relative; }}

        /* Tabs & Tables */
        .nav-tabs .nav-link {{ cursor: pointer; font-weight: 500; color: #6c757d; border: none; border-bottom: 3px solid transparent; }}
        .nav-tabs .nav-link.active {{ color: #0d6efd; border-bottom: 3px solid #0d6efd; font-weight: bold; background: none; }}

        .table-custom {{ width: 100%; font-size: 0.9rem; }}
        .table-custom thead th {{ background: #f8f9fa; border-bottom: 2px solid #dee2e6; padding: 12px; position: sticky; top: 0; z-index: 10; }}
        .row-alert-yellow {{ background-color: #fff8e1 !important; }}
        .row-alert-red {{ background-color: #ffebee !important; }}
        .separator-row td {{ background-color: #e9ecef; color: #6c757d; font-weight: bold; text-align: center; padding: 5px; font-size: 0.8rem; border-top: 2px solid #ced4da; }}

        /* Stock Color Badges (Column 1) */
        .stock-badge {{ font-weight: bold; padding: 5px 10px; border-radius: 6px; display: inline-block; min-width: 80px; text-align: center; color: #2c3e50; border: 1px solid rgba(0,0,0,0.05); }}

        /* Heatmap */
        table.heatmap-table {{ border-collapse: separate; border-spacing: 0; width: max-content; min-width: 100%; }}
        .cell-val {{ font-size: 0.85rem; text-align: center; border: 1px solid #dee2e6; height: 35px; min-width: 60px; vertical-align: middle; }}
        thead.heatmap-head {{ position: sticky; top: 0; z-index: 20; }}
        .expiry-cell {{ font-weight: bold; white-space: nowrap; position: sticky; left: 0; background: white; z-index: 30; border-right: 2px solid #ced4da; text-align: center; min-width: 100px; }}
        .closest-strike {{ border-left: 2px solid #0d6efd !important; border-right: 2px solid #0d6efd !important; background-color: rgba(13, 110, 253, 0.05); }}

        /* Insight Box */
        .insight-box {{ background: #f0f7ff; border-left: 5px solid #0d6efd; padding: 15px; margin: 15px; border-radius: 4px; font-size: 0.9rem; }}
        .insight-title {{ font-weight: bold; color: #0d6efd; margin-bottom: 5px; display: flex; align-items: center; gap: 8px; }}
        .insight-content {{ color: #495057; }}
        .highlight-data {{ font-weight: bold; color: #212529; background: #fff; padding: 2px 6px; border-radius: 4px; border: 1px solid #dee2e6; }}

        /* KPI & Legend */
        .kpi-card {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 8px; padding: 15px; margin-bottom: 20px; }}
        .kpi-title {{ color: #6c757d; font-size: 0.75rem; text-transform: uppercase; font-weight: 600; margin-bottom: 8px; }}
        .kpi-value {{ font-size: 1.5rem; font-weight: 800; color: #212529; }}

        .legend-box {{ background: #f8f9fa; padding: 15px; border-radius: 8px; border: 1px solid #dee2e6; font-size: 0.85rem; margin-bottom: 20px; }}
        .legend-title {{ font-weight: bold; margin-bottom: 10px; color: #495057; border-bottom: 1px solid #dee2e6; padding-bottom: 5px; }}
        .legend-item {{ margin-bottom: 4px; display: flex; align-items: baseline; gap: 8px; }}
        .legend-key {{ font-weight: bold; min-width: 90px; color: #0d6efd; }}

        .tag-call {{ background: #e7f1ff; color: #0d6efd; padding: 3px 8px; border-radius: 4px; font-weight: 700; font-size: 0.75rem; }}
        .tag-put {{ background: #fbecec; color: #dc3545; padding: 3px 8px; border-radius: 4px; font-weight: 700; font-size: 0.75rem; }}
    </style>
</head>
<body>
    <div class="container-fluid controls-area">
        <div class="d-flex justify-content-between align-items-center mb-3">
            <div class="d-flex align-items-center gap-3">
                <h4 class="mb-0 text-primary fw-bold">🇭🇰 Options Master</h4>
                <div id="stockSelectGroup" class="d-flex align-items-center gap-2">
                    <select id="tickerSelect" class="form-select border-primary shadow-sm" style="width: 200px; font-weight:bold;" onchange="renderTicker()">
                        <option value="" disabled selected>Loading...</option>
                    </select>
                    <div id="priceDisplay"></div>
                </div>
            </div>
            <div class="text-end">
                <div class="text-muted small">File: {source_filename}</div>
                <div class="badge bg-secondary">Generated: {datetime.datetime.now().strftime("%H:%M")}</div>
            </div>
        </div>

        <ul class="nav nav-tabs border-bottom-0" id="mainTabs">
            <li class="nav-item"><a class="nav-link active" id="tab-rank" onclick="switchTab('rank')">🏆 1. Stock Ranking</a></li>
            <li class="nav-item"><a class="nav-link" id="tab-scanner" onclick="switchTab('scanner')">🚀 2. Market Scanner</a></li>
            <li class="nav-item"><a class="nav-link" id="tab-analysis" onclick="switchTab('analysis')">📊 3. Stock Analysis</a></li>
            <li class="nav-item"><a class="nav-link" id="tab-today" onclick="switchTab('today')">🔥 4. Heatmap: Vol</a></li>
            <li class="nav-item"><a class="nav-link" id="tab-oi" onclick="switchTab('oi')">🧊 5. Heatmap: OI</a></li>
            <li class="nav-item"><a class="nav-link" id="tab-ratio" onclick="switchTab('ratio')">⚖️ 6. Heatmap: Ratio</a></li>
        </ul>
    </div>

    <div class="content-wrapper">
        <div id="contentContainer" class="scroll-container"></div>
    </div>

    <script>
        const fullData = {json_data};
        const stocksData = fullData.stocks;
        const marketScanner = fullData.market_scanner;
        const stockRanking = fullData.stock_ranking;

        let currentTicker = null;
        let currentTab = 'rank'; // Default tab adjusted to Rank

        window.onload = function() {{
            const select = document.getElementById('tickerSelect');
            const tickers = Object.keys(stocksData).sort();
            if (tickers.length === 0) {{ select.innerHTML = '<option>No Data</option>'; }} 
            else {{
                tickers.forEach(t => {{
                    const opt = document.createElement('option');
                    opt.value = t; opt.innerText = t; select.appendChild(opt);
                }});
                select.value = tickers[0];
            }}

            // Initial render based on default tab
            switchTab('rank');
        }};

        function switchTab(tab) {{
            currentTab = tab;
            document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
            document.getElementById('tab-' + tab).classList.add('active');

            const stockGroup = document.getElementById('stockSelectGroup');
            if (tab === 'scanner' || tab === 'rank') {{
                stockGroup.style.opacity = '0.3'; stockGroup.style.pointerEvents = 'none';
            }} else {{
                stockGroup.style.opacity = '1'; stockGroup.style.pointerEvents = 'auto';
            }}

            // Only render ticker specific logic if not in global views, 
            // but for global views we call renderContent directly
            if (tab === 'scanner' || tab === 'rank') {{
                renderContent();
            }} else {{
                renderTicker();
            }}
        }}

        function renderTicker() {{
            currentTicker = document.getElementById('tickerSelect').value;
            if (!currentTicker || !stocksData[currentTicker]) return;
            const params = stocksData[currentTicker].params || {{}};
            const priceVal = params['ul_price'] || 0;
            document.getElementById('priceDisplay').innerHTML = `
                <span class="badge bg-light text-dark border px-3 py-2" style="font-size:1rem;">
                    Spot: <span class="text-success">$${{parseFloat(priceVal).toFixed(2)}}</span>
                </span>
            `;
            if (currentTab !== 'scanner' && currentTab !== 'rank') renderContent();
        }}

        function renderContent() {{
            const container = document.getElementById('contentContainer');
            container.innerHTML = '';
            container.scrollTop = 0;

            if (currentTab === 'scanner') renderMarketScanner(container);
            else if (currentTab === 'rank') renderStockRanking(container);
            else if (currentTab === 'analysis') renderAnalysis(container);
            else renderHeatmap(container, currentTab);
        }}

        // ==========================================
        // RENDER FUNCTIONS
        // ==========================================

        function renderStockRanking(container) {{
            let html = `
            <div class="p-4" style="max-width: 1400px; margin: 0 auto;">
                <h5 class="fw-bold mb-3">🏆 Stock Ranking</h5>
                <div class="table-responsive bg-white border rounded">
                    <table class="table table-custom table-hover mb-0">
                        <thead><tr><th>Rank</th><th>Stock</th><th>Price</th><th>Total Option Turnover</th><th>Sentiment Flow</th><th>Net Delta</th></tr></thead>
                        <tbody>`;
            stockRanking.forEach((row, idx) => {{
                const deltaColor = row.net_delta > 0 ? '#198754' : '#dc3545';
                const sentiment = row.net_delta > 0 ? 'Bullish' : 'Bearish';
                html += `<tr>
                    <td class="fw-bold text-secondary">#${{idx + 1}}</td>
                    <td class="fw-bold text-primary">${{row.stock}}</td>
                    <td>$${{row.price.toFixed(2)}}</td>
                    <td class="fw-bold">$${{Math.round(row.total_turnover).toLocaleString()}}</td>
                    <td><div class="progress" style="height: 6px;"><div class="progress-bar bg-primary" style="width: ${{row.call_pct}}%"></div><div class="progress-bar bg-danger" style="width: ${{row.put_pct}}%"></div></div></td>
                    <td style="color: ${{deltaColor}}; font-weight:bold;">${{sentiment}}</td>
                </tr>`;
            }});
            html += `</tbody></table></div></div>`;
            container.innerHTML = html;
        }}

        function renderMarketScanner(container) {{
            // Define distinct colors for the top 5 stocks
            const palette = ['#E3F2FD', '#F3E5F5', '#E8F5E9', '#FBE9E7', '#E0F7FA']; 
            const stockColorMap = {{}};
            let colorIndex = 0;

            const uniqueStocks = [...new Set(marketScanner.map(item => item.stock_code))];
            uniqueStocks.slice(0, 5).forEach(s => {{
                stockColorMap[s] = palette[colorIndex % palette.length];
                colorIndex++;
            }});

            let html = `
            <div class="p-4" style="max-width: 1400px; margin: 0 auto;">
                <h5 class="fw-bold mb-3">🚀 Unusual Activity</h5>
                <div class="table-responsive bg-white border rounded">
                    <table class="table table-custom table-hover mb-0">
                        <thead><tr><th>Stock</th><th>Code</th><th>Type</th><th>Strike</th><th>Expiry</th><th>Price</th><th>Vol</th><th>OI</th><th>Vol/OI</th><th>Turnover</th></tr></thead>
                        <tbody>`;
            if (marketScanner.length === 0) html += `<tr><td colspan="10" class="text-center p-5 text-muted">No activity found.</td></tr>`;
            else {{
                marketScanner.forEach(row => {{
                    const typeClass = row.option_type === 'CALL' ? 'tag-call' : 'tag-put';
                    let rowClass = row.vol_oi_ratio >= 3.0 ? 'row-alert-red' : 'row-alert-yellow';
                    const sColor = stockColorMap[row.stock_code] || '#f8f9fa'; 

                    html += `<tr class="${{rowClass}}">
                        <td><span class="stock-badge" style="background-color: ${{sColor}};">${{row.stock_code}}</span></td>
                        <td class="small font-monospace">${{row.code}}</td>
                        <td><span class="${{typeClass}}">${{row.option_type}}</span></td>
                        <td class="fw-bold">${{row.Strike}}</td>
                        <td>${{row.Expiry}}</td>
                        <td>${{row.Price.toFixed(2)}}</td>
                        <td>${{row.volume.toLocaleString()}}</td>
                        <td>${{row.OpenInterest.toLocaleString()}}</td>
                        <td class="fw-bold">${{row.vol_oi_ratio}}x</td>
                        <td class="fw-bold">$${{Math.round(row.Turnover).toLocaleString()}}</td>
                    </tr>`;
                }});
            }}
            html += `</tbody></table></div></div>`;
            container.innerHTML = html;
        }}

        function renderAnalysis(container) {{
            const data = stocksData[currentTicker].analysis;
            const s = data.summary;
            const deltaClass = s.net_delta > 0 ? '#198754' : '#dc3545';

            const html = `
            <div class="p-4" style="max-width: 1400px; margin: 0 auto;">
                <div class="row g-3 mb-4">
                    <div class="col-md-3"><div class="kpi-card border-start border-4 border-primary"><div class="kpi-title">Trend (Net Delta)</div><div class="kpi-value" style="color: ${{deltaClass}}">${{s.net_delta > 0 ? 'Bullish' : 'Bearish'}}</div><div class="text-muted small">Score: ${{Math.round(s.net_delta).toLocaleString()}}</div></div></div>
                    <div class="col-md-3"><div class="kpi-card border-start border-4 border-warning"><div class="kpi-title">Major Walls (Res/Sup)</div><div class="d-flex justify-content-between"><div><span class="text-danger fw-bold">Call:</span> $${{s.call_wall}}</div><div><span class="text-success fw-bold">Put:</span> $${{s.put_wall}}</div></div><div class="text-muted small">Max OI Levels (±10 ticks)</div></div></div>
                    <div class="col-md-3"><div class="kpi-card border-start border-4 border-info"><div class="kpi-title">Exp. Range (Daily)</div><div class="kpi-value">$${{(s.spot - s.exp_move).toFixed(2)}} - $${{(s.spot + s.exp_move).toFixed(2)}}</div><div class="text-muted small">Move: ±$${{s.exp_move}}</div></div></div>
                    <div class="col-md-3"><div class="kpi-card border-start border-4 border-secondary"><div class="kpi-title">Sentiment (IV Skew)</div><div class="kpi-value">${{s.iv_skew > 0 ? '+' : ''}}${{s.iv_skew.toFixed(2)}}%</div><div class="text-muted small">${{s.iv_skew > 1 ? 'Fear (High Puts)' : 'Neutral/Greed'}}</div></div></div>
                </div>

                <div class="legend-box">
                    <div class="legend-title">📚 Metric Legend</div>
                    <div class="row">
                        <div class="col-md-6">
                            <div class="legend-item"><span class="legend-key">Net Delta:</span> Measure of total market maker exposure. Positive = Bullish flow.</div>
                            <div class="legend-item"><span class="legend-key">IV Skew:</span> Put IV minus Call IV. High Positive = High Fear (Puts expensive).</div>
                        </div>
                        <div class="col-md-6">
                            <div class="legend-item"><span class="legend-key">Vol/OI:</span> Volume to Open Interest ratio. > 1.0x is active, > 3.0x is explosive.</div>
                            <div class="legend-item"><span class="legend-key">Exp Move:</span> "Rule of 16". Theoretical daily range derived from Implied Volatility.</div>
                        </div>
                    </div>
                </div>

                <h5 class="fw-bold mb-3">🔥 Today's Top Activity</h5>
                <div class="table-responsive bg-white border rounded">
                    <table class="table table-custom mb-0">
                        <thead><tr><th>Code</th><th>Type</th><th>Strike</th><th>Expiry</th><th>Price</th><th>Volume</th><th>OI</th><th>Vol/OI</th><th>Turnover</th></tr></thead>
                        <tbody>
                            ${{data.movers.map(row => {{
                                if (row._is_separator) return `<tr class="separator-row"><td colspan="9">🔻 Turnover below $1,000,000 🔻</td></tr>`;
                                const typeClass = row.option_type === 'CALL' ? 'tag-call' : 'tag-put';
                                let rowClass = row.vol_oi_ratio >= 3.0 ? 'row-alert-red' : (row.vol_oi_ratio >= 1.0 ? 'row-alert-yellow' : '');
                                return `<tr class="${{rowClass}}"><td class="small font-monospace">${{row.code}}</td><td><span class="${{typeClass}}">${{row.option_type}}</span></td><td class="fw-bold">${{row.Strike}}</td><td>${{row.Expiry}}</td><td>${{row.Price.toFixed(2)}}</td><td>${{row.volume.toLocaleString()}}</td><td>${{row.OpenInterest.toLocaleString()}}</td><td class="fw-bold">${{row.vol_oi_ratio}}x</td><td class="fw-bold">$${{Math.round(row.Turnover).toLocaleString()}}</td></tr>`;
                            }}).join('')}}
                        </tbody>
                    </table>
                </div>
            </div>`;
            container.innerHTML = html;
        }}

        function renderHeatmap(container, tabKey) {{
            const dataObj = stocksData[currentTicker][tabKey];
            const s = stocksData[currentTicker].analysis.summary;

            if (!dataObj || !dataObj.data || dataObj.data.length === 0) {{
                container.innerHTML = '<div class="p-5 text-center text-muted">No data available.</div>';
                return;
            }}

            let insightHTML = '';
            if (tabKey === 'today') {{
                insightHTML = `
                <div class="insight-box">
                    <div class="insight-title">🔥 Turnover Heatmap: Follow the Money</div>
                    <div class="insight-content">
                        This view shows where the real money is flowing today. Dark green cells indicate heavy trading.
                        <ul class="mb-0 mt-2 ps-3">
                            <li><strong>Today's Hotspot:</strong> The most active contract is <span class="highlight-data">${{s.hotspot}}</span>.</li>
                            <li><strong>Bullish Signal:</strong> Look for high turnover on OTM Calls (Strike > Spot).</li>
                            <li><strong>Bearish Signal:</strong> Look for high turnover on OTM Puts (Strike < Spot).</li>
                        </ul>
                    </div>
                </div>`;
            }} else if (tabKey === 'oi') {{
                insightHTML = `
                <div class="insight-box">
                    <div class="insight-title">🧊 Open Interest Walls: Support & Resistance</div>
                    <div class="insight-content">
                        This view shows the market structure. Dark green cells are positions held overnight ("Walls").
                        <ul class="mb-0 mt-2 ps-3">
                            <li><strong>Resistance Wall:</strong> Call Strike <span class="highlight-data">$${{s.call_wall}}</span> has the most OI. Price often struggles to break this.</li>
                            <li><strong>Support Wall:</strong> Put Strike <span class="highlight-data">$${{s.put_wall}}</span> has the most OI. This acts as a floor.</li>
                            <li><strong>Expiry Magnet:</strong> On expiry days, price often gravitates towards these large walls (Max Pain).</li>
                        </ul>
                    </div>
                </div>`;
            }} else if (tabKey === 'ratio') {{
                insightHTML = `
                <div class="insight-box">
                    <div class="insight-title">⚖️ Vol/OI Ratio: The "Explosion" Detector</div>
                    <div class="insight-content">
                        This view highlights <strong>new</strong> aggressive positions. Ratio = Volume / Open Interest.
                        <ul class="mb-0 mt-2 ps-3">
                            <li><strong>High Ratio (> 3.0x):</strong> Means volume is 3x the existing positions. This is a potential breakout signal or major repositioning.</li>
                            <li><strong>Low Ratio (< 0.5x):</strong> Just regular trading, no major new bets.</li>
                        </ul>
                    </div>
                </div>`;
            }}

            const rows = dataObj.index;     
            const cols = dataObj.columns;   
            const values = dataObj.data;    
            const params = stocksData[currentTicker].params || {{}};
            const currentPrice = parseFloat(params['ul_price'] || 0);

            let closestIdx = -1;
            let minDiff = Infinity;
            cols.forEach((s, i) => {{
                const strikeVal = parseFloat(s);
                if (!isNaN(strikeVal) && currentPrice > 0) {{
                    const diff = Math.abs(strikeVal - currentPrice);
                    if (diff < minDiff) {{ minDiff = diff; closestIdx = i; }}
                }}
            }});

            let maxVal = 0;
            values.forEach(r => r.forEach(v => {{ if(v > maxVal) maxVal = v; }}));

            let html = insightHTML + '<div class="p-3"><table class="heatmap-table table-sm table-hover mb-0">';
            html += '<thead class="heatmap-head"><tr><th class="expiry-cell">Expiry \\ Strike</th>';
            cols.forEach((col, i) => {{
                const isClosest = (i === closestIdx);
                const cls = isClosest ? 'header-cell closest-strike' : 'header-cell';
                const idAttr = isClosest ? 'id="targetColumn"' : '';
                html += `<th class="${{cls}}" style="padding:10px; background:#f8f9fa; border:1px solid #dee2e6; text-align:center;" ${{idAttr}}>${{col}}</th>`;
            }});
            html += '</tr></thead><tbody>';

            rows.forEach((expiry, rIdx) => {{
                html += `<tr><td class="expiry-cell">${{expiry}}</td>`;
                values[rIdx].forEach((val, cIdx) => {{
                    let displayVal = val || 0;
                    let alpha = 0;
                    if (maxVal > 0 && displayVal > 0) alpha = Math.sqrt(displayVal / maxVal);
                    if (alpha > 1) alpha = 1;

                    const bg = `rgba(13, 110, 253, ${{alpha.toFixed(2)}})`;
                    const color = alpha > 0.5 ? 'white' : 'black';
                    const isClosest = (cIdx === closestIdx);
                    const cellClass = isClosest ? 'cell-val closest-strike' : 'cell-val';

                    let valStr = Math.round(displayVal).toLocaleString();
                    if (tabKey === 'ratio') valStr = displayVal.toFixed(2);

                    html += `<td class="${{cellClass}}" style="background-color: ${{bg}}; color: ${{color}};" title="${{displayVal}}">${{valStr}}</td>`;
                }});
                html += '</tr>';
            }});
            html += '</tbody></table></div>';

            container.innerHTML = html;
            setTimeout(() => {{
                const target = document.getElementById('targetColumn');
                if (target) {{ target.scrollIntoView({{ behavior: 'auto', block: 'nearest', inline: 'center' }}); }}
            }}, 100);
        }}
    </script>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"\n✅ Done! File created: {output_file}")


if __name__ == "__main__":
    target_file = find_latest_csv()
    if target_file:
        data = parse_csv_to_data(target_file)
        if data:
            generate_html(data, os.path.basename(target_file))
        else:
            print("⚠️ Parsed data is empty.")
    else:
        print(f"❌ No file found starting with '{FILE_PREFIX}' in current folder.")