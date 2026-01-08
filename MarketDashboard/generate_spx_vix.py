import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yfinance as yf
import io
import base64
from pathlib import Path
from datetime import datetime, timedelta

# ==========================================
# Configuration
# ==========================================
OUTPUT_FOLDER = Path("MarketDashboard")
OUTPUT_HTML = "spx_vix_scatter.html"


def generate_spx_vix_html():
    """
    Generates SPX vs VIX scatter plot, encodes as base64, and saves as HTML with a data table.
    """
    # Create output directory if it doesn't exist
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    end_date = datetime.now()
    start_date = end_date - timedelta(days=100)  # ~3 months buffer

    print("[INFO] Downloading SPX and VIX data...")
    # Use threads=False to avoid some yfinance issues in actions
    spx = yf.download("^GSPC", start=start_date, end=end_date, progress=False, auto_adjust=False, threads=False)
    vix = yf.download("^VIX", start=start_date, end=end_date, progress=False, auto_adjust=False, threads=False)

    if spx.empty or vix.empty:
        print("[ERROR] Failed to download data.")
        return

    # Calculate Daily % Changes
    # Handle MultiIndex columns if present (yfinance update)
    try:
        if isinstance(spx.columns, pd.MultiIndex):
            spx_close = spx["Close"]["^GSPC"]
        else:
            spx_close = spx["Close"]

        if isinstance(vix.columns, pd.MultiIndex):
            vix_close = vix["Close"]["^VIX"]
        else:
            vix_close = vix["Close"]
    except KeyError:
        # Fallback if structure is flat but different
        spx_close = spx["Close"]
        vix_close = vix["Close"]

    spx_pct = spx_close.pct_change() * 100
    vix_pct = vix_close.pct_change() * 100

    df = pd.concat([spx_pct, vix_pct], axis=1).dropna()
    df.columns = ["SPX_%", "VIX_%"]
    df = df.reset_index()  # Date becomes a column
    # Ensure Date column exists and is datetime
    if "Date" not in df.columns:
        df.rename(columns={"index": "Date"}, inplace=True)

    if len(df) < 5:
        print("[ERROR] Not enough data points.")
        return

    # ==================== Plotting ====================
    fig = plt.figure(figsize=(12, 8))

    # 1. Plot Older Points (Gray)
    plt.scatter(
        df["SPX_%"][:-5],
        df["VIX_%"][:-5],
        color="lightgray",
        alpha=0.6,
        s=60,
        label="Earlier days"
    )

    # 2. Plot Last 5 Days (Gradient Red/Orange)
    colors = ["#ffff66", "#ffaa33", "#ff6600", "#ff3300", "#cc0000"]
    labels = ["-4 days", "-3 days", "-2 days", "-1 day", "Latest"]

    for i in range(5):
        idx = -5 + i
        row = df.iloc[idx]

        plt.scatter(
            row["SPX_%"],
            row["VIX_%"],
            color=colors[i],
            s=160,
            edgecolors="black",
            linewidth=1.2,
            zorder=10
        )

        # Date Label
        date_str = row["Date"].strftime("%Y-%m-%d")
        plt.text(
            row["SPX_%"] + 0.1,
            row["VIX_%"] + 0.3,
            f"{date_str}\n{labels[i]}",
            fontsize=9,
            ha="left",
            va="bottom",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.9, edgecolor="#ccc")
        )

    # 3. Axis & Grid
    plt.axhline(0, color="black", lw=1, alpha=0.3)
    plt.axvline(0, color="black", lw=1, alpha=0.3)

    plt.xlabel("S&P 500 Daily % Change", fontsize=12, fontweight='bold')
    plt.ylabel("VIX Daily % Change", fontsize=12, fontweight='bold')
    plt.title("SPX vs VIX Daily Correlation (Last ~3 Months)", fontsize=16, fontweight='bold', pad=20)
    plt.grid(True, alpha=0.3, linestyle="--")
    plt.legend(loc="upper right")

    # 4. Quadrant Coloring (Visual Aid)
    # Get current limits to fill spans correctly
    xlims = plt.xlim()
    ylims = plt.ylim()

    # Fear Quadrant (SPX down, VIX up) - Red tint
    plt.fill_between([xlims[0], 0], 0, ylims[1], color='red', alpha=0.03)
    # Greed/Complacency (SPX up, VIX down) - Green tint
    plt.fill_between([0, xlims[1]], ylims[0], 0, color='green', alpha=0.03)

    plt.tight_layout()

    # Watermark
    plt.text(0.99, 0.01, "@ParisTrader", transform=plt.gca().transAxes,
             fontsize=12, color="gray", alpha=0.5, ha="right", va="bottom", weight="bold")

    # ==================== Save Plot to Base64 ====================
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    base64_img = base64.b64encode(buf.getvalue()).decode("utf-8")

    # ==================== Generate Data Table (Last 20 Days) ====================
    last_20 = df.tail(20).sort_values(by="Date", ascending=False)

    table_rows = ""
    for _, row in last_20.iterrows():
        date_fmt = row["Date"].strftime("%Y-%m-%d")
        spx_val = row["SPX_%"]
        vix_val = row["VIX_%"]

        # Color coding for values
        spx_color = "#10B981" if spx_val >= 0 else "#EF4444"
        vix_color = "#10B981" if vix_val >= 0 else "#EF4444"  # Green if up, Red if down (standard logic)

        table_rows += f"""
        <tr>
            <td>{date_fmt}</td>
            <td style="color:{spx_color}; font-weight:bold;">{spx_val:+.2f}%</td>
            <td style="color:{vix_color}; font-weight:bold;">{vix_val:+.2f}%</td>
        </tr>
        """

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ==================== Generate Final HTML ====================
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>SPX vs VIX Correlation</title>
        <style>
            body {{
                background-color: #0B0E14;
                color: #E2E8F0;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                display: flex;
                flex-direction: column;
                align-items: center;
                min-height: 100vh;
            }}
            .container {{
                background: #FFFFFF;
                border-radius: 16px;
                padding: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.5);
                max-width: 1000px;
                width: 95%;
                margin-bottom: 30px;
            }}
            img {{
                width: 100%;
                height: auto;
                border-radius: 8px;
            }}
            .table-container {{
                max-width: 800px;
                width: 95%;
                background: #1E293B;
                border-radius: 12px;
                padding: 20px;
                border: 1px solid #334155;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                color: #CBD5E1;
            }}
            th {{
                text-align: left;
                padding: 12px;
                border-bottom: 2px solid #475569;
                color: #94A3B8;
                font-size: 0.9em;
                text-transform: uppercase;
            }}
            td {{
                padding: 12px;
                border-bottom: 1px solid #334155;
                font-family: 'Courier New', monospace;
            }}
            tr:last-child td {{
                border-bottom: none;
            }}
            tr:hover {{
                background-color: #334155;
            }}
            h2 {{ color: #F8FAFC; margin-bottom: 10px; }}
            h3 {{ color: #F8FAFC; margin-bottom: 15px; margin-top:0; }}
            .meta {{
                margin-top: 30px;
                color: #64748B;
                font-size: 0.9em;
            }}
        </style>
    </head>
    <body>
        <h2 style="text-align:center;">S&P 500 vs VIX Analysis</h2>

        <div class="container">
            <img src="data:image/png;base64,{base64_img}" alt="SPX vs VIX Scatter Plot" />
        </div>

        <div class="table-container">
            <h3>📅 Past 20 Days Performance</h3>
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>SPX Change</th>
                        <th>VIX Change</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </div>

        <div class="meta">Last Updated: {timestamp} (Server Time)</div>
    </body>
    </html>
    """

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"[SUCCESS] Generated: {OUTPUT_HTML}")


if __name__ == "__main__":
    generate_spx_vix_html()