import pandas as pd
from curl_cffi import requests
import os
from datetime import datetime, timedelta
import traceback

# ==========================================
# 1. Configuration
# ==========================================
OUTPUT_DIR = "EconomicCalendar"
OUTPUT_FILENAME_PREFIX = "calendar_report_"


# ==========================================
# 2. Data Fetching Functions
# ==========================================

def get_next_week_dates():
    """Calculates the start (Sunday) and end (Next Sunday) of the coming week."""
    today = datetime.now()
    days_ahead = 6 - today.weekday()
    if days_ahead <= 0:
        days_ahead += 7

    start_date = today + timedelta(days=days_ahead)
    end_date = start_date + timedelta(days=7)

    return start_date, end_date


def get_economic_calendar(start_date=None, end_date=None):
    """
    Fetches calendar data from TradingView API (UTC) and converts to HK Time.
    """
    if not start_date or not end_date:
        start_date, end_date = get_next_week_dates()

    print(f"Fetching data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")

    url = "https://economic-calendar.tradingview.com/events"

    params = {
        "from": start_date.strftime("%Y-%m-%dT00:00:00.000Z"),
        "to": end_date.strftime("%Y-%m-%dT23:59:59.000Z"),
        "countries": "US,JP,CN"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Origin": "https://www.tradingview.com",
        "Referer": "https://www.tradingview.com/"
    }

    try:
        r = requests.get(url, headers=headers, params=params, impersonate="chrome110", timeout=15)

        if r.status_code == 403:
            print("[Error] 403 Forbidden - The protection is still blocking the request.")
            return pd.DataFrame()

        r.raise_for_status()
        data = r.json()

        if "result" in data:
            data = data["result"]

        df = pd.DataFrame(data)

        if df.empty:
            return pd.DataFrame()

        # [Safety Check] Ensure we have a date column
        date_col = 'date'
        if 'date' not in df.columns and 'date_time' in df.columns:
            date_col = 'date_time'

        # --- Data Processing ---
        df['dateline'] = pd.to_datetime(df[date_col])

        # CHANGED: Convert UTC to Hong Kong Time (UTC+8)
        df['dateline'] = df['dateline'] + timedelta(hours=8)

        df['DateStr'] = df['dateline'].dt.strftime('%Y-%m-%d')
        df['TimeStr'] = df['dateline'].dt.strftime('%H:%M')

        country_map = {'US': 'USD', 'JP': 'JPY', 'CN': 'CNY'}
        df['Currency'] = df['country'].map(country_map)

        def map_impact(val):
            try:
                val = int(val)
                if val >= 2: return "High"
                if val == 1: return "Medium"
            except:
                pass
            return "Low"

        df['Impact'] = df['importance'].apply(map_impact)

        # Rename columns (Changed Time (ET) to Time (HKT))
        df = df.rename(columns={
            'title': 'Event',
            'previous': 'Previous',
            'forecast': 'Forecast',
            'DateStr': 'Date',
            'TimeStr': 'Time (HKT)'
        })

        cols = ['Event', 'Currency', 'Impact', 'Date', 'Time (HKT)', 'Forecast', 'Previous']

        # Ensure all columns exist
        for c in cols:
            if c not in df.columns: df[c] = ""

        df = df[cols].fillna('-')
        df = df.sort_values(by=['Date', 'Time (HKT)'])

        return df

    except Exception as e:
        print(f"[Error] Calendar fetch failed: {e}")
        traceback.print_exc()
        return pd.DataFrame()


# ==========================================
# 3. HTML Generation
# ==========================================

def generate_html_report():
    calendar_df = get_economic_calendar()
    gen_time = datetime.now().strftime("%Y-%m-%d %H:%M")

    calendar_rows = ""
    if not calendar_df.empty:
        for _, row in calendar_df.iterrows():
            impact_badge = ""
            row_class = ""

            if "High" in str(row['Impact']):
                impact_badge = '<span class="badge badge-high">High</span>'
                row_class = "row-high"
            elif "Medium" in str(row['Impact']):
                impact_badge = '<span class="badge badge-mid">Med</span>'
            else:
                impact_badge = '<span class="badge badge-low">Low</span>'

            curr = row['Currency']
            curr_class = "curr-usd" if curr == 'USD' else ("curr-jpy" if curr == 'JPY' else "curr-cny")
            curr_badge = f'<span class="badge {curr_class}">{curr}</span>'

            calendar_rows += f"""
            <tr class="{row_class}">
                <td>{row['Date']}</td>
                <td>{row['Time (HKT)']}</td>
                <td>{curr_badge}</td>
                <td>{impact_badge}</td>
                <td style="font-weight:500;">{row['Event']}</td>
                <td>{row['Forecast']}</td>
                <td>{row['Previous']}</td>
            </tr>
            """
    else:
        calendar_rows = "<tr><td colspan='7' style='text-align:center; padding:20px;'>No events found (or connection blocked).</td></tr>"

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            :root {{ --bg-color: #0b0e14; --card-bg: #1e293b; --text-main: #e2e8f0; --text-sub: #94a3b8; --accent: #3b82f6; --border: #334155; }}
            body {{ font-family: 'Segoe UI', sans-serif; background-color: var(--bg-color); color: var(--text-main); padding: 20px; }}
            h2 {{ border-bottom: 2px solid var(--accent); padding-bottom: 10px; margin-bottom: 20px; font-size: 1.5em; }}
            table {{ width: 100%; border-collapse: collapse; background: var(--card-bg); border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
            th, td {{ padding: 12px 15px; text-align: left; border-bottom: 1px solid var(--border); font-size: 0.9em; }}
            th {{ background: #0f172a; color: var(--text-sub); font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }}
            tr:last-child td {{ border-bottom: none; }}
            .badge {{ padding: 3px 8px; border-radius: 4px; font-size: 0.75em; font-weight: bold; text-transform: uppercase; letter-spacing: 0.5px; }}
            .badge-high {{ background: rgba(239, 68, 68, 0.2); color: #fca5a5; border: 1px solid rgba(239, 68, 68, 0.5); }}
            .badge-mid {{ background: rgba(245, 158, 11, 0.2); color: #fcd34d; border: 1px solid rgba(245, 158, 11, 0.5); }}
            .badge-low {{ background: rgba(59, 130, 246, 0.2); color: #93c5fd; border: 1px solid rgba(59, 130, 246, 0.5); }}
            .curr-usd {{ background: rgba(16, 185, 129, 0.15); color: #6ee7b7; }} 
            .curr-jpy {{ background: rgba(236, 72, 153, 0.15); color: #f9a8d4; }} 
            .curr-cny {{ background: rgba(239, 68, 68, 0.15); color: #fca5a5; }} 
            .row-high {{ background: rgba(239, 68, 68, 0.08); }}
        </style>
    </head>
    <body>
        <h2>📅 Economic Calendar (Next Week: {gen_time})</h2>
        <table>
            <thead>
                <tr>
                    <th>Date</th>
                    <th>Time (HKT)</th>
                    <th>Cur</th>
                    <th>Impact</th>
                    <th>Event</th>
                    <th>Forecast</th>
                    <th>Previous</th>
                </tr>
            </thead>
            <tbody>
                {calendar_rows}
            </tbody>
        </table>
    </body>
    </html>
    """

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{OUTPUT_FILENAME_PREFIX}{timestamp}.html"
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"✅ Calendar Report saved: {filepath}")


if __name__ == "__main__":
    generate_html_report()