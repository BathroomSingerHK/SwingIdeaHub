import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os

# --- [KEY CHANGE] Setup Base Directory ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Configuration ---
URLS = {
    "Cluster_buy": "http://openinsider.com/latest-cluster-buys",
    "Insider_Buy": "http://openinsider.com/insider-purchases-25k",
    "Insider_Trading": "http://openinsider.com/latest-insider-trading"
}


# --- 1. Scraping Engine ---
def fetch_table(url):
    """Fetches the HTML table from OpenInsider and returns a DataFrame."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    print(f"Fetching data from: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        table = soup.find('table', {'class': 'tinytable'})
        if not table:
            print(f"Warning: No table found on {url}")
            return pd.DataFrame()

        # Extract headers and clean special characters
        headers = [th.text.strip().replace('\xa0', ' ') for th in table.find('thead').findAll('th')]

        # Extract rows
        rows = []
        for tr in table.find('tbody').findAll('tr'):
            row = [td.text.strip() for td in tr.findAll('td')]
            if row:
                rows.append(row)

        return pd.DataFrame(rows, columns=headers)

    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return pd.DataFrame()


# --- 2. Data Processing & Cleaning ---
def clean_currency(x):
    if not isinstance(x, str): return 0.0
    clean = x.replace('$', '').replace(',', '').replace('+', '').replace('>', '').replace('<', '').strip()
    try:
        return float(clean) if clean else 0.0
    except ValueError:
        return 0.0


def clean_percentage(x):
    if not isinstance(x, str): return 0.0
    clean = x.replace('%', '').replace('+', '').replace('>', '').replace('<', '').replace('New', '').strip()
    try:
        return float(clean) if clean else 0.0
    except ValueError:
        return 0.0


def process_dataframe(df):
    if df.empty: return df

    # Normalize column names
    df.columns = [c.replace('\xa0', ' ').strip() for c in df.columns]

    # Add numeric columns for sorting
    if 'Value' in df.columns:
        df['Value_Num'] = df['Value'].apply(clean_currency)
    if 'Own' in df.columns:  # Adjusted column name matching
        df['Delta_Own_Num'] = df['Own'].apply(clean_percentage)  # Sometimes scraped as 'Own' or 'ΔOwn'

    return df.fillna('')


# --- 3. HTML Generation ---
def generate_html_report(data_dict, timestamp_str):
    """Creates the styled HTML report."""

    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Insider Trading Report - {timestamp}</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, sans-serif; margin: 0; padding: 20px; background-color: #0B0E14; color: #e2e8f0; }}
            .container {{ max-width: 1200px; margin: 0 auto; background: #131722; padding: 30px; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.3); border: 1px solid #2a2e39; }}
            h1 {{ color: #f8fafc; text-align: center; margin-bottom: 5px; }}
            .date {{ text-align: center; color: #94a3b8; margin-bottom: 30px; font-weight: bold; font-size: 0.9em; }}
            h2 {{ color: #3498db; border-bottom: 2px solid #2a2e39; padding-bottom: 10px; margin-top: 50px; }}

            .educational-box {{ background-color: #1e293b; border-left: 5px solid #10b981; padding: 20px; margin-bottom: 25px; border-radius: 4px; }}
            .educational-box h3 {{ margin-top: 0; color: #10b981; font-size: 1.1em; }}
            .educational-box p {{ margin: 5px 0; line-height: 1.5; color: #cbd5e1; }}

            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 0.85em; }}
            th {{ background-color: #1e293b; color: #f8fafc; padding: 12px 15px; text-align: left; border-bottom: 1px solid #475569; }}
            td {{ padding: 10px 15px; border-bottom: 1px solid #2a2e39; }}
            tr:hover {{ background-color: #2d3748; }}
            .footer {{ margin-top: 50px; text-align: center; font-size: 0.85em; color: #64748b; border-top: 1px solid #2a2e39; padding-top: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Daily Insider Trading Report</h1>
            <p class="date">Generated on: {timestamp}</p>

            <div class="educational-box">
                <h3>🔑 Key Definition: High 'Ins' Count</h3>
                <p><strong>'Ins' (Insider Count)</strong> represents the number of unique insiders buying stock simultaneously.</p>
                <p>A count of <strong>3 or more</strong> is a "Cluster Buy." This high conviction signal shows that multiple leaders (CEO, CFO, Directors) agree the stock is undervalued.</p>
            </div>

            <h2>🚀 Cluster Buys (High 'Ins' Counts)</h2>
            <div class="educational-box">
                <p><strong>Strategy:</strong> Prioritize rows with high 'Ins' numbers and large total Value.</p>
            </div>
            {cluster_table}

            <h2>💼 Significant Insider Buys</h2>
            <div class="educational-box">
                <p><strong>Strategy:</strong> Look for C-Suite purchases over $100k.</p>
            </div>
            {insider_buy_table}

            <h2>📊 Latest Transactions</h2>
            {trading_table}

            <div class="footer">Data Source: OpenInsider</div>
        </div>
    </body>
    </html>
    """

    def df_to_html(df, columns):
        if df.empty: return "<p>No data available.</p>"
        # Filter columns that actually exist in the dataframe
        valid_cols = [c for c in columns if c in df.columns]
        return df[valid_cols].head(50).to_html(index=False, border=0, escape=False)

    cluster_df = data_dict.get('Cluster_buy', pd.DataFrame())
    insider_df = data_dict.get('Insider_Buy', pd.DataFrame())
    trading_df = data_dict.get('Insider_Trading', pd.DataFrame())

    # Sort
    if not cluster_df.empty and 'Value_Num' in cluster_df.columns:
        cluster_df = cluster_df.sort_values(by='Value_Num', ascending=False)
    if not insider_df.empty and 'Value_Num' in insider_df.columns:
        insider_df = insider_df.sort_values(by='Value_Num', ascending=False)
    if not trading_df.empty and 'Filing Date' in trading_df.columns:
        trading_df = trading_df.sort_values(by='Filing Date', ascending=False)

    cluster_cols = ['Filing Date', 'Ticker', 'Company Name', 'Industry', 'Ins', 'Price', 'Value']
    insider_cols = ['Filing Date', 'Ticker', 'Company Name', 'Insider Name', 'Title', 'Price', 'Qty', 'Value',
                    'Own']  # Fixed column name
    trading_cols = ['Filing Date', 'Ticker', 'Company Name', 'Insider Name', 'Trade Type', 'Price', 'Value']

    return html_template.format(
        timestamp=timestamp_str,
        cluster_table=df_to_html(cluster_df, cluster_cols),
        insider_buy_table=df_to_html(insider_df, insider_cols),
        trading_table=df_to_html(trading_df, trading_cols)
    )


# --- 4. Main Execution ---
if __name__ == "__main__":
    print("Starting Insider Data Analysis...")

    # Generate timestamp for filename
    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d %H:%M")
    filename_timestamp = now.strftime("%Y-%m-%d_%H-%M")

    # [KEY] Save to BASE_DIR
    output_filename = f"Insider_Trading_Report_{filename_timestamp}.html"
    output_path = os.path.join(BASE_DIR, output_filename)

    # Fetch Data
    data_store = {}
    for name, url in URLS.items():
        df = fetch_table(url)
        data_store[name] = process_dataframe(df)
        print(f"Loaded {len(df)} rows for {name}")

    # Generate & Save
    html_content = generate_html_report(data_store, timestamp_str)

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"\nSUCCESS! Report saved to: {output_path}")
    except Exception as e:
        print(f"Error saving file: {e}")