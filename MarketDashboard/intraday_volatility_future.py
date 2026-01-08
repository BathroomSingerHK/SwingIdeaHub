import base64
import html
import io
import platform
import sys
from datetime import datetime
from pathlib import Path
import warnings

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import pandas as pd
import yfinance as yf

# Suppress pandas future warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- CONFIGURATION ---
OUTPUT_DIR = Path(__file__).resolve().parent
TARGET_TIMEZONE = 'Asia/Hong_Kong'

# MASTER ASSET CONFIGURATION
ASSETS = [
    {
        'name': 'Hang Seng Index (HSI)',
        'ticker_daily': '^HSI',
        'ticker_intraday': '^HSI',
        'desc': 'HK Market (Day Only)',
        'is_24h': False
    },
    {
        'name': 'S&P 500 (SPX)',
        'ticker_daily': '^GSPC',
        'ticker_intraday': 'ES=F',
        'desc': 'US Large Cap (Futures View)',
        'is_24h': True
    },
    {
        'name': 'Nasdaq 100 (NDX)',
        'ticker_daily': '^NDX',
        'ticker_intraday': 'NQ=F',
        'desc': 'US Tech Giants (Futures View)',
        'is_24h': True
    },
    {
        'name': 'Gold Futures (Gold)',
        'ticker_daily': 'GC=F',
        'ticker_intraday': 'GC=F',
        'desc': '24h Global Market',
        'is_24h': True
    }
]


# --- UTILS ---

def set_fonts():
    """Configures fonts for Matplotlib to support Chinese characters and special symbols."""
    system_name = platform.system()
    if system_name == "Windows":
        plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'SimHei', 'Arial']
    elif system_name == "Darwin":  # Mac
        plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'PingFang HK', 'Heiti TC']
    else:  # Linux/Server
        plt.rcParams['font.sans-serif'] = ['WenQuanYi Micro Hei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False


def fig_to_base64(fig):
    """Converts a matplotlib figure to a base64 string."""
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')


# --- PART 1: TRUE RANGE LOGIC (Daily Volatility) ---

def get_daily_data(asset):
    symbol = asset['ticker_daily']
    print(f"   [Daily] Fetching data for {symbol}...")

    if symbol == 'GC=F':
        try:
            df_hourly = yf.download(symbol, period="59d", interval="60m", progress=False, auto_adjust=True)
            if df_hourly.empty: return pd.DataFrame()
            if isinstance(df_hourly.columns, pd.MultiIndex):
                df_hourly.columns = df_hourly.columns.get_level_values(0)

            df_hourly['DateStr'] = df_hourly.index.date
            daily_df = df_hourly.groupby('DateStr').agg({
                'High': 'max', 'Low': 'min', 'Close': 'last'
            })
            daily_df.index = pd.to_datetime(daily_df.index)
            daily_df.sort_index(inplace=True)
            return daily_df
        except Exception as e:
            print(f"   [Error] Failed to fetch hourly GC=F: {e}")
            return pd.DataFrame()
    else:
        df = yf.download(symbol, period="60d", interval="1d", progress=False, auto_adjust=True)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df


def generate_true_range_chart(asset):
    set_fonts()
    df = get_daily_data(asset)

    if df.empty or len(df) < 5:
        print(f"   [Daily] Not enough data for {asset['name']}")
        return None

    # Calculate Metrics
    df['Range'] = df['High'] - df['Low']
    df['SMA_5'] = df['Range'].rolling(window=5).mean()

    # Slice last 30 days
    df = df.iloc[-30:].copy()

    avg_volatility = df['Range'].mean()
    current_vol = df['Range'].iloc[-1]

    is_active = current_vol > avg_volatility
    status_text = "Status: ACTIVE (Volatile)" if is_active else "Status: QUIET"
    status_color = '#c0392b' if is_active else '#7f8c8d'
    caption_status = "⚠️ Active/Volatile" if is_active else "✅ Quiet"

    # Plotting
    fig, ax = plt.subplots(figsize=(12, 6))

    bar_colors = ['#bdc3c7' if r < avg_volatility else '#e74c3c' for r in df['Range']]
    bars = ax.bar(df.index, df['Range'], color=bar_colors, alpha=0.85)

    # Lines
    ax.axhline(y=avg_volatility, color='blue', linestyle='--', linewidth=2, alpha=0.8)
    ax.plot(df.index, df['SMA_5'], color='black', linewidth=2.5, marker='o', markersize=4, zorder=5)

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2., height + (height * 0.02),
                f'{int(height)}', ha='center', va='bottom', fontsize=8)

    ax.set_title(f"{asset['name']} - 30 Day Volatility", fontsize=14, weight='bold')
    ax.set_ylabel('True Range (Points)')
    ax.grid(True, axis='y', linestyle='--', alpha=0.3)

    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    legend_elements = [
        mpatches.Patch(color='#e74c3c', label='Above Avg'),
        mpatches.Patch(color='#bdc3c7', label='Below Avg'),
        plt.Line2D([0], [0], color='blue', lw=2, linestyle='--', label=f'30-Day Avg ({int(avg_volatility)})'),
        plt.Line2D([0], [0], color='black', lw=2, marker='o', label='5-Day Trend')
    ]
    ax.legend(handles=legend_elements, loc='upper left', framealpha=0.9, fontsize='small')

    ax.text(0.98, 0.95, status_text, transform=ax.transAxes,
            fontsize=12, weight='bold', color='white', ha='right', va='top',
            bbox=dict(boxstyle="round,pad=0.4", facecolor=status_color, edgecolor='none', alpha=0.9))

    plt.tight_layout()

    # --- UPDATED CAPTION FORMAT ---
    caption = (
        f"📊 <b>{asset['name']}</b> Volatility Report<br>"
        f"📅 Date: {df.index[-1].strftime('%Y-%m-%d')}<br>"
        f"📉 Current Range: {int(current_vol)}<br>"
        f"📏 30-Day Avg: {int(avg_volatility)}<br>"
        f"📢 {caption_status}"
    )

    return {
        'image_b64': fig_to_base64(fig),
        'caption': caption,
        'date': df.index[-1].strftime('%Y-%m-%d')
    }


# --- PART 2: TRADING TIME LOGIC (Intraday Profile) ---

def plot_intraday_zones(asset):
    set_fonts()
    symbol = asset['ticker_intraday']
    print(f"   [Intraday] Fetching data for {symbol}...")

    try:
        df = yf.download(symbol, period="59d", interval="15m", progress=False, auto_adjust=True)
    except Exception as e:
        print(f"Error downloading {symbol}: {e}")
        return None

    if df.empty: return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
    df.index = df.index.tz_convert(TARGET_TIMEZONE)

    df = df.dropna(subset=['High', 'Low'])
    df = df[df['High'] > 0]

    df['Range'] = df['High'] - df['Low']
    df['TimeStr'] = df.index.strftime('%H:%M')

    intraday_vol = df.groupby('TimeStr')['Range'].mean()
    if intraday_vol.empty or intraday_vol.sum() == 0: return None

    threshold_grey = intraday_vol.quantile(0.50)
    threshold_red = intraday_vol.quantile(0.80)

    colors = []
    for val in intraday_vol.values:
        if val >= threshold_red:
            colors.append('#c0392b')
        elif val <= threshold_grey:
            colors.append('#95a5a6')
        else:
            colors.append('#f39c12')

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(intraday_vol.index, intraday_vol.values, color=colors, alpha=0.9, width=0.8)

    locator_interval = 2 if len(intraday_vol) < 40 else 4
    ax.xaxis.set_major_locator(mticker.MultipleLocator(locator_interval))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", fontsize=8)

    ax.set_title(f"{asset['name']} - Intraday Zones ({TARGET_TIMEZONE})", fontsize=14, weight='bold')
    ax.set_ylabel('Avg 15m Range (Points)')
    ax.set_xlabel(f'Time of Day ({TARGET_TIMEZONE})')
    ax.grid(True, axis='y', linestyle='--', alpha=0.3)

    ax.axhline(y=threshold_grey, color='gray', linestyle=':', linewidth=1.5)
    ax.text(0, threshold_grey, ' Trap Zone Limit', color='gray', fontsize=8, va='bottom')

    plt.tight_layout()

    best_time = intraday_vol.idxmax()

    # --- UPDATED CAPTION FORMAT ---
    # Add specific note for HSI since it is spot data
    note = "(註：使用現貨數據，僅涵蓋日間盤 09:30-16:00)<br>" if 'HSI' in asset['name'] else ""

    caption = (
        f"🔥 <b>{asset['name']} Intraday Volatility Guide</b><br>"
        f"🕒 Timezone: {TARGET_TIMEZONE}<br>"
        f"🎯 <b>Best Hunting Time:</b> {best_time}<br>"
        f"{note}<br>"
        f"🟥 <b>Red Bars (Hunt Zone - Top 20%):</b><br>"
        f"菁英時段。動能充足，適合 Breakout，TP 觸發率高。<br><br>"
        f"⬜ <b>Grey Bars (Trap Zone - Bottom 50%):</b><br>"
        f"垃圾時間/陷阱區。<br>"
        f"⚠️ <b>Why Avoid?</b><br>"
        f"1. Range 太窄，利潤不夠付點差。<br>"
        f"2. 無方向震盪，易掃 SL。<br>"
        f"3. 浪費時間。<br><br>"
        f"👉 <b>建議：灰色時段嚴格空倉。</b><br>"
    )

    return {
        'image_b64': fig_to_base64(fig),
        'caption': caption,
        'desc': asset['desc']
    }


# --- PART 3: HTML GENERATION ---

def build_html_report(tr_results, tt_results):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def make_section(title, result_map, is_tr=True):
        html_cards = ""
        for asset in ASSETS:
            name = asset['name']
            if name not in result_map:
                continue

            data = result_map[name]

            if is_tr:
                card = f"""
                <div class="card">
                    <div class="card-header">
                        <h3>{html.escape(name)}</h3>
                        <span class="badge">{data['date']}</span>
                    </div>
                    <img src="data:image/png;base64,{data['image_b64']}" alt="{name}">
                    <div class="caption-box">
                        {data['caption']}
                    </div>
                </div>"""
            else:
                card = f"""
                <div class="card">
                    <div class="card-header">
                        <h3>{html.escape(name)}</h3>
                        <span class="subtitle">{html.escape(data.get('desc', ''))}</span>
                    </div>
                    <img src="data:image/png;base64,{data['image_b64']}" alt="{name}">
                    <div class="caption-box">
                        {data['caption']}
                    </div>
                </div>"""
            html_cards += card

        return f"""
        <section>
            <h2>{title}</h2>
            {html_cards if html_cards else '<p>No data available.</p>'}
        </section>
        """

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Intraday Volatility Dashboard</title>
    <style>
        :root {{ --bg: #f4f7f6; --card: #ffffff; --text: #2c3e50; --accent: #3498db; --border: #ecf0f1; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 20px; }}
        .container {{ max-width: 1000px; margin: 0 auto; }}
        header {{ text-align: center; margin-bottom: 40px; border-bottom: 2px solid #dfe6e9; padding-bottom: 20px; }}
        h1 {{ margin: 0; color: #2c3e50; }}
        .ts {{ color: #7f8c8d; font-size: 0.9em; margin-top: 5px; }}

        section {{ margin-bottom: 60px; }}
        h2 {{ border-left: 5px solid var(--accent); padding-left: 15px; margin-bottom: 20px; font-size: 1.5em; background: #e8eaf6; padding: 10px 15px; border-radius: 0 5px 5px 0; }}

        .card {{ background: var(--card); border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); padding: 20px; margin-bottom: 30px; border: 1px solid var(--border); }}
        .card-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; }}
        .card h3 {{ margin: 0; font-size: 1.2em; }}
        .subtitle {{ font-size: 0.9em; color: #95a5a6; font-style: italic; }}
        .badge {{ background: #ecf0f1; padding: 4px 8px; border-radius: 4px; font-size: 0.85em; font-weight: bold; color: #7f8c8d; }}

        img {{ width: 100%; height: auto; display: block; border-radius: 4px; border: 1px solid #f1f2f6; }}

        .caption-box {{ margin-top: 15px; background: #f8f9fa; padding: 15px; border-radius: 6px; font-size: 0.95em; line-height: 1.6; border-left: 3px solid #bdc3c7; color: #2c3e50; }}
        .caption-box b {{ color: #2980b9; }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Volatility & Intraday Dashboard</h1>
            <div class="ts">Generated: {timestamp} | Timezone: {TARGET_TIMEZONE}</div>
        </header>

        {make_section("1. True Range Analysis (Daily Volatility)", tr_results, is_tr=True)}
        {make_section("2. Trading Time / Intraday Profile (15m Average)", tt_results, is_tr=False)}

        <footer style="text-align:center; color:#bdc3c7; margin-top:50px; font-size:0.8em;">
        </footer>
    </div>
</body>
</html>"""

    filename = "Intraday_Volatility.html"
    out_path = OUTPUT_DIR / filename
    out_path.write_text(html_content, encoding='utf-8')
    return out_path


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    tr_results = {}
    tt_results = {}

    print("=== Starting Dashboard Generation ===")

    for asset in ASSETS:
        print(f"\nProcessing Asset: {asset['name']}")

        # 1. True Range
        try:
            res = generate_true_range_chart(asset)
            if res: tr_results[asset['name']] = res
        except Exception as e:
            print(f" -> [Error] Daily Range: {e}")
            import traceback
            traceback.print_exc()

        # 2. Trading Time
        try:
            res = plot_intraday_zones(asset)
            if res: tt_results[asset['name']] = res
        except Exception as e:
            print(f" -> [Error] Intraday: {e}")
            import traceback
            traceback.print_exc()

    print("\nGenerating HTML Report...")
    try:
        path = build_html_report(tr_results, tt_results)
        print(f"SUCCESS: Dashboard saved to:\n{path}")
        try:
            print(f"URI: {path.resolve().as_uri()}")
        except:
            pass
    except Exception as e:
        print(f"ERROR generating HTML: {e}")


if __name__ == "__main__":
    main()