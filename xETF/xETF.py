import pandas as pd
import yfinance as yf
import os
import sys
from datetime import datetime

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILENAME = os.path.join(BASE_DIR, "etf_list.csv")

# --- HTML TEMPLATE ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>ETF Smart Money Tracker</title>
    <meta charset="utf-8">
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.11.5/css/jquery.dataTables.css">
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f6f9; padding: 20px; color: #333; }}
        .container {{ max-width: 98%; margin: 0 auto; background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); }}

        h1 {{ color: #2c3e50; margin-bottom: 5px; border-bottom: 2px solid #eee; padding-bottom: 15px; }}
        .subtitle {{ color: #7f8c8d; font-size: 0.95em; margin-top: 5px; margin-bottom: 20px; }}

        /* --- LAYOUT GRID --- */
        .tables-row {{ display: flex; justify-content: space-between; gap: 20px; margin-bottom: 40px; }}
        .table-column {{ width: 49%; display: flex; flex-direction: column; }}

        h2 {{ margin-top: 0; margin-bottom: 15px; color: #34495e; font-size: 1.3em; display: flex; align-items: center; border-bottom: 2px solid #f0f0f0; padding-bottom: 10px; }}
        .badge {{ padding: 5px 10px; border-radius: 4px; font-size: 0.8em; font-weight: bold; margin-right: 10px; }}
        .badge-pos {{ background-color: #e8f5e9; color: #2e7d32; border: 1px solid #c8e6c9; }}
        .badge-neg {{ background-color: #ffebee; color: #c62828; border: 1px solid #ffcdd2; }}

        /* Compact Table Styling */
        table.dataTable {{ width: 100% !important; border-collapse: collapse; font-size: 0.85em; }}
        th {{ background-color: #2c3e50; color: white; padding: 8px; text-align: left; font-weight: 600; }}
        td {{ padding: 6px 8px; border-bottom: 1px solid #eee; }}

        /* Educational Section */
        .edu-section {{ background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin-top: 10px; margin-bottom: 30px; border-left: 5px solid #2980b9; }}
        .edu-title {{ font-size: 1.2em; font-weight: bold; color: #2980b9; margin-bottom: 15px; }}
        .edu-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; }}
        .edu-box h3 {{ font-size: 1em; color: #444; margin-bottom: 8px; }}
        .edu-box p {{ font-size: 0.9em; line-height: 1.5; color: #666; }}

        /* --- COLLAPSIBLE TABLE STYLES (NEW) --- */
        .collapsible-wrapper {{
            position: relative;
            max-height: 450px; /* Default collapsed height (~15 rows) */
            overflow: hidden;
            transition: max-height 0.5s ease;
            border-bottom: 1px solid #eee;
        }}

        .collapsible-wrapper.expanded {{
            max-height: none; /* Remove limit when expanded */
            overflow: visible;
        }}

        /* Fade effect at the bottom when collapsed */
        .fade-overlay {{
            position: absolute;
            bottom: 0;
            left: 0;
            width: 100%;
            height: 80px;
            background: linear-gradient(to bottom, rgba(255,255,255,0), rgba(255,255,255,1));
            pointer-events: none;
            z-index: 5;
        }}

        .expand-btn {{
            display: block;
            width: 100%;
            padding: 10px;
            margin-top: -1px; /* Overlap border */
            background-color: #f1f3f5;
            color: #2c3e50;
            text-align: center;
            border: none;
            cursor: pointer;
            font-weight: bold;
            font-size: 0.9em;
            border-radius: 0 0 8px 8px;
            transition: background 0.2s;
        }}
        .expand-btn:hover {{ background-color: #e2e6ea; }}

        /* --- HEATMAP STYLES --- */
        .heatmap-container {{ margin-top: 40px; overflow-x: auto; max-height: 800px; }}
        .heatmap-table {{ width: 100%; border-collapse: separate; border-spacing: 0; font-size: 0.8em; }}
        .heatmap-table th, .heatmap-table td {{ padding: 4px; text-align: center; border: 1px solid #ddd; }}
        .heatmap-table th {{ background-color: #34495e; color: white; white-space: nowrap; position: sticky; top: 0; z-index: 20; }}
        .heatmap-ticker {{ text-align: left !important; font-weight: bold; background-color: #f9f9f9; position: sticky; left: 0; z-index: 10; border-right: 1px solid #ccc; }}
        .heatmap-underlying {{ text-align: left !important; background-color: #f9f9f9; position: sticky; left: 60px; z-index: 10; border-right: 2px solid #333; }}
        .group-start td {{ border-top: 3px solid #333 !important; }}
        .heatmap-cell {{ width: 45px; height: 25px; cursor: default; color: #333; }}

        /* Heatmap Legend */
        .heatmap-legend {{
            margin-top: 15px; padding: 15px; background-color: #fff;
            border: 1px solid #eee; border-radius: 8px; font-size: 0.9em; color: #555;
            display: flex; align-items: center; gap: 20px;
        }}
        .legend-item {{ display: flex; align-items: center; gap: 8px; }}
        .color-box {{ width: 20px; height: 20px; border-radius: 4px; border: 1px solid #ccc; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 Leveraged ETF Smart Money Tracker</h1>
        <p class="subtitle">Generated: {gen_time}</p>

        <div class="edu-section">
            <div class="edu-title">🎓 Guide</div>
            <div class="edu-grid">
                <div class="edu-box">
                    <h3>1. Grouping</h3>
                    <p>ETFs are grouped by <strong>Underlying</strong>. Groups are sorted by the single hottest ETF within them.</p>
                </div>
                <div class="edu-box">
                    <h3>2. Turnover</h3>
                    <p><strong>>200% (Green)</strong> = High conviction move. <br>The heatmap opacity represents volume intensity.</p>
                </div>
                <div class="edu-box">
                    <h3>3. Trend</h3>
                    <p><strong>Green Cell</strong> = Price Up.<br><strong>Red Cell</strong> = Price Down.</p>
                </div>
            </div>
        </div>

        <div class="tables-row">
            <div class="table-column">
                <h2><span class="badge badge-pos">BULLISH</span> Positive Momentum (Today)</h2>
                <div class="collapsible-wrapper" id="posWrapper">
                    {table_pos_html}
                    <div class="fade-overlay"></div>
                </div>
                <button class="expand-btn" onclick="toggleTable('posWrapper', this)">⬇️ Show All (Expand)</button>
            </div>

            <div class="table-column">
                <h2><span class="badge badge-neg">BEARISH</span> Negative Momentum (Today)</h2>
                <div class="collapsible-wrapper" id="negWrapper">
                    {table_neg_html}
                    <div class="fade-overlay"></div>
                </div>
                <button class="expand-btn" onclick="toggleTable('negWrapper', this)">⬇️ Show All (Expand)</button>
            </div>
        </div>

        <h2>📅 20-Day Volume & Trend Heatmap (Sorted by T-Day Activity)</h2>
        <div class="heatmap-legend">
            <strong>📊 Heatmap Guide:</strong>
            <div class="legend-item">
                <div class="color-box" style="background-color: rgba(0, 150, 0, 1.0);"></div>
                <span>Price Up + High Vol</span>
            </div>
            <div class="legend-item">
                <div class="color-box" style="background-color: rgba(0, 150, 0, 0.3);"></div>
                <span>Price Up + Low Vol</span>
            </div>
            <div class="legend-item">
                <div class="color-box" style="background-color: rgba(220, 20, 60, 1.0);"></div>
                <span>Price Down + High Vol</span>
            </div>
            <div class="legend-item">
                <div class="color-box" style="background-color: rgba(220, 20, 60, 0.3);"></div>
                <span>Price Down + Low Vol</span>
            </div>
            <div style="margin-left: auto; font-style: italic;">
                * Numbers in cells represent Relative Volume % (Turnover)
            </div>
        </div>
        <div class="heatmap-container">
            {heatmap_html}
        </div>
    </div>

    <script src="https://code.jquery.com/jquery-3.5.1.js"></script>
    <script src="https://cdn.datatables.net/1.11.5/js/jquery.dataTables.js"></script>
    <script>
        $(document).ready(function() {{
            var tableSettings = {{
                "order": [[ 5, "desc" ]],
                "pageLength": 100, // Show many rows, but let CSS hide them
                "searching": true,
                "info": false,
                "lengthChange": false,
                "paging": false
                // REMOVED scrollY to allow CSS max-height to work properly
            }};
            $('#tablePos').DataTable(tableSettings);
            $('#tableNeg').DataTable(tableSettings);

            // Initial check: if table is short, hide the button
            checkHeight('posWrapper');
            checkHeight('negWrapper');
        }});

        function toggleTable(wrapperId, btn) {{
            var wrapper = document.getElementById(wrapperId);
            var overlay = wrapper.querySelector('.fade-overlay');

            if (wrapper.classList.contains('expanded')) {{
                // Collapse
                wrapper.classList.remove('expanded');
                overlay.style.display = 'block';
                btn.innerHTML = "⬇️ Show All (Expand)";
                // Optional: Scroll back to top of table
                wrapper.scrollIntoView({{behavior: 'smooth', block: 'start'}});
            }} else {{
                // Expand
                wrapper.classList.add('expanded');
                overlay.style.display = 'none';
                btn.innerHTML = "⬆️ Show Less (Collapse)";
            }}
        }}

        function checkHeight(wrapperId) {{
            var wrapper = document.getElementById(wrapperId);
            var table = wrapper.querySelector('table');
            var btn = wrapper.nextElementSibling;
            // If table is shorter than max-height (450px), hide button and overlay
            if (table.clientHeight < 450) {{
                btn.style.display = 'none';
                wrapper.querySelector('.fade-overlay').style.display = 'none';
                wrapper.style.borderBottom = 'none';
            }}
        }}
    </script>
</body>
</html>
"""


def load_etf_list(csv_path):
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found.")
        sys.exit(1)
    try:
        df = pd.read_csv(csv_path)
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)


def download_data(tickers):
    print(f"Fetching data for {len(tickers)} ETFs...")
    try:
        data = yf.download(tickers, period="6mo", group_by='ticker', progress=True, threads=True)
        return data
    except Exception as e:
        print(f"Fatal Error downloading data: {e}")
        return None


def process_summary_data(data, tickers, underlying_map):
    results = []
    is_multi_level = isinstance(data.columns, pd.MultiIndex)

    for ticker in tickers:
        try:
            if is_multi_level:
                if ticker not in data.columns.levels[0]: continue
                df = data[ticker]
            else:
                if len(tickers) == 1:
                    df = data
                else:
                    continue

            if df.empty or len(df) < 2: continue

            latest = df.iloc[-1]
            prev = df.iloc[-2]
            price = latest['Close']
            if pd.isna(price): continue

            change_pct = ((price - prev['Close']) / prev['Close']) * 100
            # User modification retained: using tail(20)
            avg_vol = df['Volume'].tail(20).mean()
            curr_vol = latest['Volume']
            turnover_pct = (curr_vol / avg_vol) * 100 if avg_vol > 0 else 0

            results.append({
                "Ticker": ticker,
                "Underlying": underlying_map.get(ticker, ""),
                "Price": price,
                "Change": change_pct,
                "Volume": curr_vol,
                "Turnover": turnover_pct
            })
        except Exception as e:
            continue

    return pd.DataFrame(results)


def generate_heatmap_html(data, tickers, underlying_map, days_back=20):
    print("Generating Heatmap...")

    # 1. Determine Date Range from master data
    master_df = None
    is_multi_level = isinstance(data.columns, pd.MultiIndex)
    for ticker in tickers:
        if is_multi_level:
            if ticker in data.columns.levels[0]:
                master_df = data[ticker]
                break
        else:
            master_df = data
            break

    if master_df is None or master_df.empty: return "<p>No data</p>"
    dates = master_df.index[-days_back:][::-1]  # Reverse order (Latest first)

    # Identify the Target "T" Date for sorting
    target_date = dates[0]
    print(f"Sorting heatmap by date: {target_date.date()}")

    # 2. Pre-calculate metrics for sorting and grouping
    rows_data = []

    for ticker in tickers:
        if is_multi_level:
            if ticker not in data.columns.levels[0]: continue
            df = data[ticker]
        else:
            df = data

        if df.empty: continue

        underlying = underlying_map.get(ticker, "Other")

        # Calculate T-Day Turnover for sorting
        try:
            if target_date in df.index:
                latest_vol = df['Volume'].loc[target_date]
                if pd.isna(latest_vol): latest_vol = 0
            else:
                latest_vol = 0

            avg_vol_63 = df['Volume'].tail(63).mean()
            t_turnover = (latest_vol / avg_vol_63 * 100) if (avg_vol_63 > 0) else 0
        except:
            t_turnover = 0

        rows_data.append({
            "ticker": ticker,
            "underlying": underlying,
            "t_turnover": t_turnover,
            "df": df
        })

    # 3. Grouping and Sorting Logic
    groups = {}
    for row in rows_data:
        u = row['underlying']
        if u not in groups: groups[u] = []
        groups[u].append(row)

    # Calculate Max Turnover per Group
    group_max_turnover = {}
    for u, rows in groups.items():
        if not rows:
            group_max_turnover[u] = 0
            continue
        max_t = max([r['t_turnover'] for r in rows])
        group_max_turnover[u] = max_t
        # Sort tickers WITHIN the group by turnover
        rows.sort(key=lambda x: x['t_turnover'], reverse=True)

    # Sort GROUPS by their max turnover
    sorted_groups = sorted(groups.items(), key=lambda item: group_max_turnover[item[0]], reverse=True)

    # 4. Generate HTML
    html = '<table class="heatmap-table">'

    # Header
    html += '<thead><tr>'
    html += '<th class="heatmap-ticker" style="min-width: 60px;">Ticker</th>'
    html += '<th class="heatmap-underlying" style="min-width: 60px;">Undl</th>'
    for i, date in enumerate(dates):
        d_str = date.strftime('%m/%d')
        label = "T" if i == 0 else f"T-{i}"
        html += f'<th title="{date.strftime("%Y-%m-%d")}">{label}<br><span style="font-weight:normal; font-size:0.8em">{d_str}</span></th>'
    html += '</tr></thead><tbody>'

    # Body
    for underlying, rows in sorted_groups:
        is_first_in_group = True

        for row in rows:
            ticker = row['ticker']
            df = row['df']

            tr_class = 'class="group-start"' if is_first_in_group else ''

            html += f'<tr {tr_class}>'
            html += f'<td class="heatmap-ticker">{ticker}</td>'
            html += f'<td class="heatmap-underlying">{underlying}</td>'

            # User requested 20-day average consistency
            avg_vol_series = df['Volume'].rolling(window=20).mean()

            for date in dates:
                if date not in df.index:
                    html += '<td style="background-color: #fcfcfc;">-</td>'
                    continue

                try:
                    loc_idx = df.index.get_loc(date)
                    if loc_idx < 1:
                        html += '<td>-</td>'
                        continue

                    curr_vol = df['Volume'].iloc[loc_idx]
                    avg_vol = avg_vol_series.iloc[loc_idx]
                    curr_price = df['Close'].iloc[loc_idx]
                    prev_price = df['Close'].iloc[loc_idx - 1]

                    if pd.isna(avg_vol) or avg_vol == 0:
                        turnover = 0
                    else:
                        turnover = (curr_vol / avg_vol * 100)

                    if pd.isna(prev_price) or prev_price == 0:
                        change_pct = 0
                    else:
                        change_pct = ((curr_price - prev_price) / prev_price * 100)

                    # Styles
                    if change_pct >= 0:
                        r, g, b = 0, 150, 0
                    else:
                        r, g, b = 220, 20, 60

                    alpha = (turnover - 50) / 250
                    alpha = max(0.1, min(alpha, 1.0))

                    if turnover < 50:
                        bg_style = 'background-color: rgba(240,240,240,0.5); color: #ccc;'
                    else:
                        text_c = 'white' if alpha > 0.5 else 'black'
                        bg_style = f'background-color: rgba({r},{g},{b},{alpha:.2f}); color: {text_c}; font-weight:bold;'

                    tooltip = f"Date: {date.strftime('%Y-%m-%d')}&#10;Price: ${curr_price:.2f}&#10;Change: {change_pct:+.2f}%&#10;Vol: {turnover:.0f}%"
                    html += f'<td class="heatmap-cell" style="{bg_style}" title="{tooltip}">{turnover:.0f}</td>'
                except:
                    html += '<td>err</td>'

            html += '</tr>'
            is_first_in_group = False

    html += '</tbody></table>'
    return html


# --- HELPERS ---
def style_turnover(val):
    if val < 100: return ''
    intensity = min((val - 100) / 200, 1.0)
    r = int(144 * (1 - intensity))
    g = int(238 - (138 * intensity))
    b = int(144 * (1 - intensity))
    text_c = 'white' if intensity > 0.6 else 'black'
    return f'background-color: rgb({r},{g},{b}); color: {text_c}; font-weight: bold;'


def style_change_pos(val):
    if val <= 0: return ''
    intensity = min(val / 5, 1.0)
    bg = f'rgba(30, 144, 255, {0.2 + (0.8 * intensity)})'
    return f'background-color: {bg}; color: white;'


def style_change_neg(val):
    if val >= 0: return ''
    intensity = min(abs(val) / 5, 1.0)
    bg = f'rgba(255, 69, 0, {0.2 + (0.8 * intensity)})'
    return f'background-color: {bg}; color: white;'


def format_table(df, table_id, is_positive):
    df = df.sort_values(by='Turnover', ascending=False)
    styler = df.style.format({
        "Price": "${:.2f}",
        "Change": "{:+.2f}%",
        "Volume": "{:,.0f}",
        "Turnover": "{:.0f}%"
    })
    styler.map(style_turnover, subset=['Turnover'])
    if is_positive:
        styler.map(style_change_pos, subset=['Change'])
    else:
        styler.map(style_change_neg, subset=['Change'])

    styler.set_table_attributes(f'id="{table_id}" class="display"')
    try:
        styler.hide(axis='index')
    except:
        styler.hide_index()
    return styler.to_html()


def main():
    etf_list_df = load_etf_list(CSV_FILENAME)
    tickers = etf_list_df['Symbol'].unique().tolist()
    underlying_map = dict(zip(etf_list_df['Symbol'], etf_list_df['Underlying']))

    raw_data = download_data(tickers)
    if raw_data is None or raw_data.empty: return

    summary_df = process_summary_data(raw_data, tickers, underlying_map)

    html_pos, html_neg = "<p>No Data</p>", "<p>No Data</p>"
    if not summary_df.empty:
        df_pos = summary_df[summary_df['Change'] >= 0].copy()
        df_neg = summary_df[summary_df['Change'] < 0].copy()
        html_pos = format_table(df_pos, "tablePos", is_positive=True)
        html_neg = format_table(df_neg, "tableNeg", is_positive=False)

    heatmap_html = generate_heatmap_html(raw_data, tickers, underlying_map, days_back=20)

    final_html = HTML_TEMPLATE.format(
        gen_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        table_pos_html=html_pos,
        table_neg_html=html_neg,
        heatmap_html=heatmap_html
    )

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"ETF_Smart_Money_Report_{timestamp_str}.html"
    output_path = os.path.join(BASE_DIR, output_filename)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_html)

    print(f"Report saved: {output_path}")


if __name__ == "__main__":
    main()