import datetime
import pandas as pd
import yfinance as yf
import time
import os
import json
from curl_cffi import requests as curl_requests

# ================= Configuration =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = BASE_DIR
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# 設定下週時間
today = datetime.date.today()
days_to_monday = (7 - today.weekday()) % 7 or 7
next_monday = today + datetime.timedelta(days=days_to_monday)
next_friday = next_monday + datetime.timedelta(days=4)
dates = [(next_monday + datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(5)]

print(f"Generating Report for Week: {next_monday} to {next_friday}")


# ================= PART 1: Fetch Earnings (Nasdaq) =================
def get_earnings_data():
    print("--- Fetching Earnings ---")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/"
    }
    all_earnings = []

    for date in dates:
        url = f"https://api.nasdaq.com/api/calendar/earnings?date={date}"
        try:
            # 使用 curl_requests 繞過簡單的 TLS 指紋檢查
            r = curl_requests.get(url, headers=headers, impersonate="chrome110", timeout=10)
            data = r.json().get("data", {}).get("rows", [])
            if data:
                print(f"   {date}: Found {len(data)} tickers")
            for row in data:
                all_earnings.append({
                    "Date": date,
                    "Ticker": row["symbol"].strip(),
                    "Company": row.get("name", "N/A").strip(),
                })
        except Exception as e:
            print(f"   Warning: Failed to fetch {date}: {e}")

    if not all_earnings:
        print("   No earnings data found.")
        return pd.DataFrame()

    df = pd.DataFrame(all_earnings)

    # Enrich with Market Cap
    print(f"   Enriching {len(df)} tickers (filtering for major caps)...")
    results = []

    # 為了避免 YFinance 卡住，我們分批處理或只處理看起來正常的 Ticker
    for ticker in df["Ticker"]:
        # 簡單過濾：含有特殊字符的往往是權證或優先股，跳過以節省時間
        if len(ticker) > 5 or "^" in ticker:
            results.append({"MarketCap": 0, "Sector": "N/A"})
            continue

        try:
            # 獲取市值
            info = yf.Ticker(ticker).info
            mcap = info.get("marketCap", 0)
            # 如果是 None (有些 ETF 或少數股會回傳 None)，設為 0
            if mcap is None: mcap = 0

            results.append({
                "MarketCap": mcap,
                "Sector": info.get("sector", "N/A")
            })
            time.sleep(0.05)  # 稍微禮貌一點，避免被 Yahoo 封鎖
        except:
            results.append({"MarketCap": 0, "Sector": "N/A"})

    extra = pd.DataFrame(results)
    df = pd.concat([df, extra], axis=1)

    # Filtering & Sorting
    df = df.dropna(subset=["MarketCap"])
    df = df[df["MarketCap"] >= 2_000_000_000]  # 過濾：只看 20億美金以上 (小票太亂)

    # 建立顯示用的欄位，保留原始數值欄位用於排序和高亮
    df["MarketCap_Display"] = df["MarketCap"].apply(lambda x: f"${x / 1_000_000_000:,.1f}B")

    df = df.sort_values(by=["Date", "MarketCap"], ascending=[True, False])

    # 返回所有需要的欄位，包括原始 MarketCap (之後會隱藏)
    return df[["Date", "Ticker", "Company", "Sector", "MarketCap_Display", "MarketCap"]]


# ================= PART 2: Fetch Econ Data (TradingView) =================
def get_econ_data():
    print("--- Fetching Econ Data ---")
    start_dt = datetime.datetime.combine(next_monday, datetime.time.min)
    end_dt = datetime.datetime.combine(next_friday, datetime.time.max)

    url = "https://economic-calendar.tradingview.com/events"
    params = {
        "from": start_dt.strftime("%Y-%m-%dT00:00:00.000Z"),
        "to": end_dt.strftime("%Y-%m-%dT23:59:59.000Z"),
        "countries": "US,CN"
    }

    try:
        # 加強 Headers 偽裝
        headers = {
            "Origin": "https://www.tradingview.com",
            "Referer": "https://www.tradingview.com/"
        }
        r = curl_requests.get(url, headers=headers, params=params, impersonate="chrome110", timeout=15)

        # 安全解析 JSON
        try:
            data = r.json()
        except json.JSONDecodeError:
            print("   [Error] TradingView returned invalid JSON (Blocked?). Skipping Econ Data.")
            return pd.DataFrame()

        result_list = data.get("result", [])
        if not result_list:
            return pd.DataFrame()

        df = pd.DataFrame(result_list)

        # Process
        date_col = 'date_time' if 'date_time' in df.columns else 'date'
        df['dateline'] = pd.to_datetime(df[date_col]) + datetime.timedelta(hours=8)  # HKT
        df['Date'] = df['dateline'].dt.strftime('%Y-%m-%d')
        df['Time'] = df['dateline'].dt.strftime('%H:%M')

        def map_impact(val):
            try:
                v = int(val)
                if v >= 2: return "High"
                if v == 1: return "Med"
            except:
                pass
            return "Low"

        df['Impact'] = df['importance'].apply(map_impact)
        df['Country'] = df['country']

        # 只保留 High/Med Impact
        df = df[df['Impact'].isin(['High', 'Med'])]

        # 確保 forecast/previous 欄位存在
        if 'forecast' not in df.columns: df['forecast'] = ""
        if 'previous' not in df.columns: df['previous'] = ""

        return df[['Date', 'Time', 'Country', 'title', 'Impact', 'forecast', 'previous']].rename(
            columns={'title': 'Event'})

    except Exception as e:
        print(f"   [Error] Econ fetch failed completely: {e}")
        return pd.DataFrame()


# ================= Execute =================
df_earnings = get_earnings_data()
df_econ = get_econ_data()


# ================= Generate Merged HTML =================

# 1. Earnings Styling
def highlight_earnings(row):
    # 這裡直接讀取 row 裡的數值，不需要額外傳 data
    try:
        mc = row["MarketCap"]
        if mc > 50_000_000_000:  # > 50B Highlight
            return ['background-color: #fff3cd; font-weight:bold; color: #856404;'] * len(row)
        return [''] * len(row)
    except:
        return [''] * len(row)


# 如果 Earnings 是空的，建立一個空的 HTML 佔位
if not df_earnings.empty:
    earnings_html = df_earnings.style.apply(highlight_earnings, axis=1) \
        .hide(subset=['MarketCap'], axis=1) \
        .set_table_attributes('class="table table-sm table-hover" style="font-size:0.85rem;"') \
        .hide(axis=0).to_html()
else:
    earnings_html = "<p class='p-3 text-muted'>No major earnings this week.</p>"


# 2. Econ Styling
def highlight_econ(row):
    styles = []
    # Impact Highlight
    if row["Impact"] == "High":
        styles.append('background-color: #f8d7da; color: #721c24; font-weight:bold;')
    elif row["Country"] == "CN":
        styles.append('background-color: #d1ecf1; color: #0c5460;')
    else:
        styles.append('')

    # 複製樣式給整行
    return [styles[0]] * len(row)


if not df_econ.empty:
    econ_html = df_econ.style.apply(highlight_econ, axis=1) \
        .set_table_attributes('class="table table-sm table-bordered" style="font-size:0.85rem;"') \
        .hide(axis=0).to_html()
else:
    econ_html = "<p class='p-3 text-muted'>No data or connection blocked.</p>"

# HTML Template
html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Weekly Outlook</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body {{ background-color: #f4f6f9; padding: 20px; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }}
        .header-box {{ background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); color: white; padding: 25px; border-radius: 8px; margin-bottom: 25px; box-shadow: 0 4px 15px rgba(0,0,0,0.2); }}
        .card {{ box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: none; margin-bottom: 20px; background: white; border-radius: 10px; overflow: hidden; }}
        .card-header {{ font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; font-size: 0.95rem; padding: 15px; }}
        .bg-econ {{ background-color: #3b3f5c; color: white; border-bottom: 3px solid #4a69bd; }}
        .bg-earn {{ background-color: #3b3f5c; color: white; border-bottom: 3px solid #f6b93b; }}
        .table-responsive {{ max-height: 850px; overflow-y: auto; scrollbar-width: thin; }}
        thead th {{ position: sticky; top: 0; z-index: 10; background: #e9ecef; color: #495057; font-size: 0.8rem; text-transform: uppercase; }}
        .badge-custom {{ font-size: 0.75rem; padding: 4px 8px; border-radius: 4px; }}
    </style>
</head>
<body>

<div class="container-fluid">
    <div class="header-box text-center">
        <h2 class="mb-1">WEEKLY OUTLOOK</h2>
        <h5 class="opacity-75">Week of {next_monday.strftime('%B %d')} - {next_friday.strftime('%B %d, %Y')}</h5>
        <div class="mt-3">
            <span class="badge bg-light text-dark mx-1">Macro Data (US/CN)</span>
            <span class="badge bg-warning text-dark mx-1">Earnings >$2B</span>
        </div>
    </div>

    <div class="row">
        <div class="col-lg-5">
            <div class="card h-100">
                <div class="card-header bg-econ">
                    📅 Economic Calendar (High/Med)
                </div>
                <div class="card-body p-0 table-responsive">
                    {econ_html}
                </div>
            </div>
        </div>

        <div class="col-lg-7">
            <div class="card h-100">
                <div class="card-header bg-earn">
                    💰 Key Earnings Releases
                </div>
                <div class="card-body p-0 table-responsive">
                    {earnings_html}
                </div>
            </div>
        </div>
    </div>

    <div class="text-center text-muted mt-5 mb-3">
        <small style="font-size: 0.7rem;">
            Generated by Bathroom Analytics • {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}<br>
        </small>
    </div>
</div>

</body>
</html>
"""

filename = f"Bathroom_NQ_EarningsWeek_of_{next_monday.strftime('%Y%m%d')}.html"
output_path = os.path.join(OUTPUT_DIR, filename)

with open(output_path, "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"\n✅ Merged Report Saved: {output_path}")