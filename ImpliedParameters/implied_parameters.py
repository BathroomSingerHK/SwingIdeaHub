import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import io
import base64
import os
from datetime import datetime, timedelta

# --- 1. CONFIGURATION (設定指標參數與定義) ---
metrics_config = {
    'VIX': {
        'ticker': '^VIX',
        'min': 10, 'max': 35,
        'def': '<b>VIX (恐慌指數)</b><br>衡量未來30天市場預期的波動程度。<br>數值越高代表投資人預期會有大漲或大跌。'
    },
    'Skew': {
        'ticker': '^SKEW',
        'min': 0, 'max': 100,
        'def': '<b>Skew (黑天鵝指數)</b><br>衡量市場對「崩盤保險」(Put) 的需求。<br>數值越高，代表防範暴跌的避險成本越貴。'
    },
    'VVIX': {
        'ticker': '^VVIX',
        'min': 70, 'max': 120,
        'def': '<b>VVIX (波動率的波動)</b><br>衡量 VIX 指數本身的不穩定程度。<br>可以用來判斷 VIX 是否即將出現劇烈跳動。'
    },
    'VIX-VIX3M': {
        'ticker': None,
        'min': -5, 'max': 5,
        'def': '<b>期限結構 (Term Structure)</b><br>比較「短期恐慌」與「中期恐慌」。<br>正常情況下長期風險應高於短期，若倒掛代表現在有危機。'
    }
}

data = {}


# --- 2. HELPER FUNCTIONS ---

def get_latest_price(ticker):
    try:
        df = yf.Ticker(ticker).history(period='1d')
        if not df.empty:
            return df['Close'].iloc[-1].item()
    except:
        return 0.0
    return 0.0


def calculate_percentile(ticker):
    try:
        hist = yf.Ticker(ticker).history(period='1y')
        if hist.empty: return 0.0
        current = hist['Close'].iloc[-1].item()
        hist = hist[hist['Close'] > 0]
        days_lower = hist[hist['Close'] < current].shape[0]
        return (days_lower / hist.shape[0]) * 100
    except:
        return 0.0


def get_market_insight(name, value):
    c_red = "#ef5350"
    c_orange = "#ffca28"
    c_green = "#66bb6a"
    c_blue = "#42a5f5"
    c_text = "#b2b5be"

    if name == 'VIX':
        if value < 15.78: return ("過度安逸 (Complacency)", "市場對風險毫無防備，需提防市場突然反轉。", c_blue)
        if value < 20: return ("正常波動 (Normal)", "市場處於健康的波動範圍。", c_green)
        if value < 24: return ("情緒緊張 (Elevated Fear)", "市場開始感到不安，避險需求增加。", c_orange)
        return ("極度恐慌 (Panic Mode)", "市場正處於拋售潮，投資人不計代價購買保護。", c_red)

    if name == 'Skew':
        if value < 20: return ("毫無避險意識 (Bullish)", "幾乎沒有人在買崩盤保險(Put)，市場一面倒看多。", c_blue)
        if value < 80: return ("正常避險 (Normal)", "機構正常的買進避險保護。", c_text)
        return ("黑天鵝警戒 (High Tail Risk)", "機構正在瘋狂搶購崩盤保險(Put)。暗示大戶極度擔心崩盤。", c_red)

    if name == 'VVIX':
        if value < 85: return ("結構穩定 (Stable)", "恐慌指數(VIX)本身很穩定。", c_green)
        if value < 110: return ("波動加劇 (Shifting)", "VIX 開始變得不穩定，暗示市場趨勢可能即將改變。", c_orange)
        return ("極不穩定 (Volatile)", "VIX 指數隨時可能暴衝。這代表風險極難預測，建議減少槓桿。", c_red)

    if name == 'VIX-VIX3M':
        if value < -1: return ("結構健康 (Contango)", "短期風險低於長期風險，這是牛市的正常狀態。", c_green)
        if value <= 0: return (
        "趨勢轉平 (Flattening)", "短期恐慌開始上升，市場猶豫不決，需密切觀察是否轉為倒掛。", c_orange)
        return (
        "結構倒掛 (Inverted / Danger)", "警報！短期恐慌大於長期恐慌。這代表市場正在發生立即性的危機，通常伴隨股市重挫。",
        c_red)

    return ("", "", c_text)


# --- 3. GENERATE SPX vs VIX DATA & CHART ---
def generate_market_data_and_chart():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=100)

    print("Fetching SPX/VIX history...")
    try:
        spx = yf.download("^GSPC", start=start_date, end=end_date, progress=False, auto_adjust=False, threads=False)
        vix = yf.download("^VIX", start=start_date, end=end_date, progress=False, auto_adjust=False, threads=False)

        try:
            spx_close = spx["Close"]["^GSPC"] if isinstance(spx.columns, pd.MultiIndex) else spx["Close"]
            vix_close = vix["Close"]["^VIX"] if isinstance(vix.columns, pd.MultiIndex) else vix["Close"]
        except:
            spx_close, vix_close = spx["Close"], vix["Close"]

        df = pd.DataFrame()
        df['SPX_Pct'] = spx_close.pct_change() * 100
        df['VIX_Level'] = vix_close
        df['VIX_Pct'] = vix_close.pct_change() * 100

        df = df.dropna().reset_index()
        if "Date" not in df.columns: df.rename(columns={"index": "Date"}, inplace=True)

        if len(df) < 5: return None, None

        # --- PLOTTING ---
        fig = plt.figure(figsize=(10, 6), facecolor='#131722')
        ax = plt.gca()
        ax.set_facecolor('#131722')

        plt.scatter(df["SPX_Pct"][:-5], df["VIX_Pct"][:-5], color="#555555", alpha=0.5, s=50)

        colors = ["#ffff66", "#ffaa33", "#ff6600", "#ff3300", "#cc0000"]
        labels = ["-4d", "-3d", "-2d", "-1d", "Latest"]

        for i in range(5):
            idx = -5 + i
            row = df.iloc[idx]
            plt.scatter(row["SPX_Pct"], row["VIX_Pct"], color=colors[i], s=120, edgecolors="white", linewidth=0.8,
                        zorder=10)
            plt.text(row["SPX_Pct"] + 0.15, row["VIX_Pct"], labels[i], color=colors[i], fontsize=9, fontweight='bold')

        ax.spines['bottom'].set_color('#444444')
        ax.spines['top'].set_color('#444444')
        ax.spines['right'].set_color('#444444')
        ax.spines['left'].set_color('#444444')
        ax.tick_params(axis='x', colors='#cccccc')
        ax.tick_params(axis='y', colors='#cccccc')
        plt.axhline(0, color="#666666", lw=1, linestyle="--")
        plt.axvline(0, color="#666666", lw=1, linestyle="--")
        plt.title("SPX vs VIX Correlation (Last 100 Days)", color="white", fontsize=14, pad=15)
        plt.xlabel("S&P 500 Daily % Change", color="#aaaaaa")
        plt.ylabel("VIX Daily % Change", color="#aaaaaa")
        plt.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=100, facecolor='#131722')
        plt.close(fig)
        buf.seek(0)
        img_str = base64.b64encode(buf.getvalue()).decode("utf-8")

        return img_str, df.tail(20).sort_values(by="Date", ascending=False)

    except Exception as e:
        print(f"Error generating chart: {e}")
        return None, None


# --- 4. MAIN LOGIC ---
print("Fetching Market Risk Data...")

data['VIX'] = round(get_latest_price('^VIX'), 2)
data['VVIX'] = round(get_latest_price('^VVIX'), 2)
data['Skew'] = round(calculate_percentile('^SKEW'), 2)

vix3m = get_latest_price('^VIX3M')
if data['VIX'] and vix3m:
    data['VIX-VIX3M'] = round(data['VIX'] - vix3m, 2)
else:
    data['VIX-VIX3M'] = 0.0

scatter_img, table_df = generate_market_data_and_chart()


# --- 5. HTML GENERATION ---
def calculate_bar_pct(value, min_val, max_val):
    if value <= min_val: return 0
    if value >= max_val: return 100
    return ((value - min_val) / (max_val - min_val)) * 100


html_rows = ""
for name, config in metrics_config.items():
    val = data.get(name, 0)
    pct = calculate_bar_pct(val, config['min'], config['max'])
    title, desc, color = get_market_insight(name, val)
    definition = config['def']

    html_rows += f"""
    <div class="row">
        <div class="col-label">
            <span class="label-text">{name}</span>
            <div class="tooltip-container">
                <div class="info-icon">i</div>
                <div class="tooltip-text">{definition}</div>
            </div>
        </div>
        <div class="col-chart">
            <div class="value">{val}</div>
            <div class="bar-container">
                <div class="gradient-bar"></div>
                <div class="marker" style="left: {pct}%;">▼</div>
            </div>
        </div>
        <div class="col-text" style="border-left: 3px solid {color};">
            <div class="status-title" style="color: {color};">{title}</div>
            <div class="status-desc">{desc}</div>
        </div>
    </div>
    """

chart_html = ""
if scatter_img:
    chart_html = f"""
    <div class="section-divider"></div>
    <div class="chart-section">
        <h3>SPX vs VIX Correlation Analysis</h3>
        <div style="text-align:center;">
            <img src="data:image/png;base64,{scatter_img}" style="max-width:100%; border-radius:8px; border:1px solid #333;">
        </div>
    </div>
    """

table_html = ""
if table_df is not None and not table_df.empty:
    table_rows = ""
    for _, row in table_df.iterrows():
        date_str = row['Date'].strftime('%Y-%m-%d')
        spx_pct = row['SPX_Pct']
        vix_lvl = row['VIX_Level']
        vix_pct = row['VIX_Pct']

        c_spx = "#4caf50" if spx_pct >= 0 else "#ef5350"
        c_vix = "#4caf50" if vix_pct >= 0 else "#ef5350"

        table_rows += f"""
        <tr>
            <td>{date_str}</td>
            <td style="color:{c_spx};">{spx_pct:+.2f}%</td>
            <td style="color:#e1e3e6;">{vix_lvl:.2f}</td>
            <td style="color:{c_vix};">{vix_pct:+.2f}%</td>
        </tr>
        """

    table_html = f"""
    <div class="table-section">
        <h3>Recent Market Data (Last 20 Days)</h3>
        <table>
            <thead>
                <tr>
                    <th>Date</th>
                    <th>SPX Change %</th>
                    <th>VIX Level</th>
                    <th>VIX Change %</th>
                </tr>
            </thead>
            <tbody>
                {table_rows}
            </tbody>
        </table>
    </div>
    """

# Add Timestamp
now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

final_html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{
            background-color: #0b0e14;
            color: #ffffff;
            font-family: "Microsoft JhengHei", "Heiti TC", sans-serif;
            margin: 0; padding: 20px;
            display: flex; justify-content: center;
        }}
        .card {{
            background-color: #131722;
            border: 1px solid #2a2e39;
            border-radius: 8px;
            padding: 30px;
            width: 900px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.6);
        }}
        h2 {{ text-align: center; color: #e1e3e6; margin-top: 0; margin-bottom: 10px; }}
        .updated-time {{ text-align: center; color: #64748b; font-size: 0.9em; margin-bottom: 30px; }}
        h3 {{ color: #b2b5be; font-size: 18px; margin-bottom: 15px; font-weight:600; border-left: 4px solid #2962ff; padding-left: 10px; }}

        .section-divider {{ height: 1px; background-color: #2a2e39; margin: 30px 0; }}

        /* Metric Rows */
        .row {{ display: flex; align-items: center; margin-bottom: 25px; }}
        .col-label {{ width: 130px; display: flex; align-items: center; }}
        .label-text {{ font-weight: 700; color: #b2b5be; margin-right: 8px; font-size: 16px; }}
        .tooltip-container {{ position: relative; cursor: pointer; }}
        .info-icon {{ width: 18px; height: 18px; background: #2a2e39; color: #787b86; border-radius: 50%; font-size: 12px; display: flex; justify-content: center; align-items: center; }}
        .tooltip-text {{ visibility: hidden; width: 250px; background: #1e222d; color: #d1d4dc; padding: 12px; position: absolute; z-index: 100; bottom: 140%; left: 50%; transform: translateX(-20%); opacity: 0; transition: opacity 0.3s; border-radius: 6px; pointer-events: none; border: 1px solid #363a45; font-size: 13px; }}
        .tooltip-container:hover .tooltip-text {{ visibility: visible; opacity: 1; }}

        .col-chart {{ width: 250px; margin-right: 40px; }}
        .value {{ font-size: 18px; font-weight: bold; margin-bottom: 8px; text-align: right; font-family: monospace; }}
        .bar-container {{ position: relative; width: 100%; height: 8px; background: #2a2e39; border-radius: 4px; }}
        .gradient-bar {{ width: 100%; height: 100%; border-radius: 4px; background: linear-gradient(90deg, #26a69a 0%, #ffeb3b 50%, #ef5350 100%); }}
        .marker {{ position: absolute; top: -16px; color: #fff; transform: translateX(-50%); font-size: 14px; text-shadow: 0 1px 2px black; }}

        .col-text {{ flex-grow: 1; padding-left: 20px; display: flex; flex-direction: column; justify-content: center; }}
        .status-title {{ font-size: 15px; font-weight: 700; margin-bottom: 4px; text-transform: uppercase; }}
        .status-desc {{ font-size: 14px; color: #9db2bd; line-height: 1.4; }}

        /* Chart Section */
        .chart-section {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #2a2e39; }}

        /* Table Styles */
        .table-section {{ margin-top: 30px; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
        th {{ text-align: left; padding: 10px; color: #787b86; border-bottom: 1px solid #434651; font-weight: 600; }}
        td {{ padding: 10px; border-bottom: 1px solid #2a2e39; font-family: monospace; }}
        tr:hover {{ background-color: #1e222d; }}
    </style>
</head>
<body>
    <div class="card">
        <h2>標普美盤風險儀表板 (Risk Dashboard)</h2>
        <div class="updated-time">Last Updated: {now_str} (UTC)</div>
        {html_rows}
        {chart_html}
        {table_html}
    </div>
</body>
</html>
"""

# [關鍵修正] 使用 BASE_DIR 確保檔案一定存到 ImpliedParameters 資料夾
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_filename = f"implied_params_{file_timestamp}.html"
output_path = os.path.join(BASE_DIR, output_filename)

with open(output_path, "w", encoding="utf-8") as f:
    f.write(final_html)

print(f"\n[Success] Risk Dashboard updated: {output_path}")