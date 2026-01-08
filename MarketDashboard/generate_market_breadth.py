import matplotlib

matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import PercentFormatter
import pandas as pd
import yfinance as yf
import io
import base64
from pathlib import Path
from datetime import datetime
import os

# ==========================================
# 1. Configuration & Stock Lists
# ==========================================

HSI_STOCKS = sorted(list(set([
    "0005.HK", "0700.HK", "9988.HK", "1299.HK", "0939.HK", "2318.HK", "3690.HK", "1211.HK",
    "1810.HK", "9618.HK", "9961.HK", "9999.HK", "9888.HK", "1024.HK", "2269.HK", "3968.HK",
    "2020.HK", "0388.HK", "0001.HK", "0002.HK", "0003.HK", "0006.HK", "0011.HK", "0012.HK",
    "0016.HK", "0019.HK", "0066.HK", "0083.HK", "0267.HK", "0688.HK", "0823.HK", "0883.HK",
    "1088.HK", "1109.HK", "1378.HK", "1876.HK", "1928.HK", "1929.HK", "1997.HK", "2015.HK",
    "2382.HK", "2388.HK", "2628.HK", "2899.HK", "3328.HK", "3692.HK", "6098.HK", "6690.HK",
    "6862.HK", "9616.HK", "9688.HK", "9901.HK", "9966.HK", "9985.HK", "9992.HK", "1816.HK",
    "1918.HK", "2313.HK", "2319.HK", "3690.HK", "3968.HK", "6618.HK", "9999.HK", "1024.HK",
    "1810.HK", "9992.HK", "2269.HK", "9888.HK", "9988.HK", "0700.HK", "9618.HK",
])))

NQ_STOCKS = [
    "NVDA", "AAPL", "MSFT", "AVGO", "AMZN", "GOOG", "TSLA", "META", "NFLX", "COST", "AMD",
    "PLTR", "CSCO", "MU", "TMUS", "PEP", "LIN", "ISRG", "QCOM", "LRCX", "INTU", "AMGN",
    "AMAT", "SHOP", "APP", "BKNG", "INTC", "GILD", "KLAC", "TXN", "ADBE", "PANW", "CRWD",
    "HON", "ADI", "VRTX", "CEG", "MELI", "ADP", "CMCSA", "SBUX", "PDD", "CDNS", "ASML",
    "ORLY", "DASH", "MAR", "CTAS", "MRVL", "MDLZ", "REGN", "SNPS", "MNST", "CSX", "AEP",
    "ADSK", "TRI", "FTNT", "PYPL", "DDOG", "WBD", "IDXX", "MSTR", "ROST", "ABNB", "AZN",
    "EA", "PCAR", "WDAY", "NXPI", "ROP", "BKR", "XEL", "ZS", "FAST", "EXC", "AXON", "TTWO",
    "FANG", "CCEP", "PAYX", "CPRT", "KDP", "CTSH", "GEHC", "VRSK", "KHC", "MCHP", "CSGP",
    "ODFL", "CHTR", "TEAM", "BIIB", "DXCM", "LULU", "ON", "ARM", "CDW", "TTD", "GFS",
]

# Output Configuration
# We will resolve the full path relative to this script to ensure stability
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FOLDER = BASE_DIR / "MarketBreadth"


# ==========================================
# 2. Plotting Logic
# ==========================================

def get_plot_as_base64(stocks, index_ticker, title):
    """
    Downloads data, calculates breadth, plots, and returns the image as a base64 string.
    """
    print(f"[INFO] Processing {title} ({index_ticker})...")

    # 1. Download Data
    tickers_to_download = stocks + [index_ticker]
    try:
        data = yf.download(tickers_to_download, period="2y", progress=False, threads=True)
    except Exception as e:
        print(f"[ERROR] Download failed for {title}: {e}")
        return None

    if data.empty:
        print(f"[ERROR] No data for {title}")
        return None

    try:
        close = data["Close"]
    except KeyError:
        print(f"[ERROR] 'Close' column missing for {title}")
        return None

    # Handle MultiIndex if present
    if index_ticker not in close.columns:
        print(f"[ERROR] Index {index_ticker} missing in data")
        return None

    # 2. Calculate Breadth
    index_series = close[index_ticker]
    # Filter constituents that are actually in the downloaded columns
    constituents = [t for t in stocks if t in close.columns]

    sma20 = close[constituents].rolling(20).mean()
    above = close[constituents] > sma20
    valid = close[constituents].notna() & sma20.notna()

    # Avoid division by zero
    valid_counts = valid.sum(axis=1)
    breadth_pct = ((above & valid).sum(axis=1) / valid_counts * 100).dropna()

    # Align dates
    index_series = index_series.reindex(breadth_pct.index).dropna()

    # 3. Plotting
    fig, ax1 = plt.subplots(figsize=(18, 10), facecolor="white")

    # Index Level (Black Line)
    ax1.plot(index_series.index, index_series, color="black", linewidth=1.3)
    ax1.set_ylabel("Index Level", color="black", fontsize=16, fontweight="bold")
    ax1.tick_params(axis="y", labelcolor="black")

    # Breadth % (Red Line)
    ax2 = ax1.twinx()
    ax2.plot(breadth_pct.index, breadth_pct, color="#d32f2f", linewidth=1.6)
    ax2.set_ylabel("Breadth (%)", color="#d32f2f", fontsize=16, fontweight="bold")
    ax2.tick_params(axis="y", labelcolor="#d32f2f")
    ax2.set_ylim(0, 100)
    ax2.yaxis.set_major_formatter(PercentFormatter())

    # Styling
    plt.title(f"{title} Market Breadth", fontsize=26, fontweight="bold", pad=40)


    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.set_facecolor("#fafafa")
    ax1.grid(True, color="white", linewidth=1.2, alpha=0.8)

    for ax in [ax1, ax2]:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    plt.tight_layout()

    # 4. Save to Buffer and Encode
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    base64_img = base64.b64encode(buf.getvalue()).decode("utf-8")
    return base64_img


# ==========================================
# 3. Main HTML Generation
# ==========================================

def generate_html_report():
    # Ensure directory exists
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    # Generate Charts
    hsi_img = get_plot_as_base64(HSI_STOCKS, "^HSI", "Hang Seng Index")
    nq_img = get_plot_as_base64(NQ_STOCKS, "^NDX", "Nasdaq 100")

    display_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # [KEY CHANGE] Generate filename with timestamp
    file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"market_breadth_{file_timestamp}.html"
    output_path = OUTPUT_FOLDER / output_filename

    # HTML Template
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Market Breadth Dashboard</title>
        <style>
            body {{
                background-color: #0B0E14;
                color: #E2E8F0;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                text-align: center;
            }}
            .container {{
                max_width: 1200px;
                margin: 0 auto;
            }}
            .header {{
                margin-bottom: 30px;
                border-bottom: 1px solid #333;
                padding-bottom: 20px;
            }}
            h1 {{ margin: 0; color: #F8FAFC; }}
            p {{ color: #94A3B8; font-size: 0.9em; }}
            .chart-box {{
                background: #FFFFFF;
                border-radius: 12px;
                padding: 10px;
                margin-bottom: 40px;
                box-shadow: 0 4px 15px rgba(0,0,0,0.3);
            }}
            img {{
                width: 100%;
                height: auto;
                border-radius: 8px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <p>Generated on: {display_time}</p>
                <p>Indicator: % of Constituents > SMA 20 (Red Line)</p>
                <p>HSI<20% often signals a market bottom/>90% often signals a market top</p>
            </div>

            <div class="chart-box">
                {'<img src="data:image/png;base64,' + hsi_img + '" />' if hsi_img else '<h3 style="color:red">Failed to generate HSI Chart</h3>'}
            </div>

                <p>NQ<20% often signals a market bottom/>80% often signals a market top</p>
            <div class="chart-box">
                {'<img src="data:image/png;base64,' + nq_img + '" />' if nq_img else '<h3 style="color:red">Failed to generate NQ Chart</h3>'}
            </div>

            <p style="color: #666; font-size: 0.8em;">Bathroom Quant Research</p>
        </div>
    </body>
    </html>
    """

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"[SUCCESS] HTML Report generated at: {output_path}")


if __name__ == "__main__":
    generate_html_report()