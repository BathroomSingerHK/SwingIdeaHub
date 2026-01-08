import asyncio
import json
import os
import re
import time
import warnings
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np
import requests
import yfinance as yf
from playwright.async_api import async_playwright

# Suppress pandas warnings
warnings.filterwarnings("ignore")

# --- CONFIGURATION ---
# Note: In GitHub Actions, cookies might not be available or valid.
# The script gracefully handles missing cookies by returning 0 hype score.
COOKIES_PATH = "cookies.json"
CRAWL_DAYS = 1

# 1. Market Cap Filter (>$50M) & High Short Float (>20%)
FINVIZ_URL = "https://finviz.com/screener.ashx?v=111&f=sh_float_short_o20,cap_microover&o=-shortfloat"

# 2. X Scan Settings
TOP_X_SCAN_LIMIT = 50

# 3. Minimum Market Cap (in Millions)
MIN_MKT_CAP_MM = 100


class RegSHO:
    """Downloads official Nasdaq Regulation SHO Threshold List."""

    def __init__(self):
        self.base_url = "http://www.nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth{date}.txt"
        self.sho_list = self._get_sho_list()

    def _get_sho_list(self):
        print("[RegSHO] Fetching official Threshold List...")
        for i in range(3):
            d = datetime.utcnow() - timedelta(days=i)
            date_str = d.strftime("%Y%m%d")
            try:
                r = requests.get(self.base_url.format(date=date_str), timeout=5)
                if r.status_code == 200:
                    lines = r.text.splitlines()
                    symbols = {line.split("|")[0].strip().upper() for line in lines if "|" in line and len(line) > 5}
                    print(f"[RegSHO] Loaded list for {date_str}: {len(symbols)} tickers.")
                    return symbols
            except:
                continue
        return set()

    def is_on_list(self, ticker):
        return ticker in self.sho_list


class DataScanner:
    """Scrapes Finviz with Stealth Mode."""

    def __init__(self):
        self.finviz_url = FINVIZ_URL
        self.mw_url = "https://www.marketwatch.com/tools/screener/short-interest"
        self.hsi_url = "https://www.highshortinterest.com/"

    async def get_most_shorted_stocks(self):
        tickers = []
        async with async_playwright() as p:
            args = ['--disable-blink-features=AutomationControlled', '--no-sandbox',
                    '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36']
            browser = await p.chromium.launch(headless=True, args=args)
            context = await browser.new_context(viewport={"width": 1280, "height": 800})
            # Block images/fonts to speed up
            await context.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2}", lambda route: route.abort())
            page = await context.new_page()

            # 1. Finviz
            try:
                print(f"[DataScanner] Trying Finviz (Filter: >$50M Cap, >20% Short)...")
                await page.goto(self.finviz_url, timeout=30000, wait_until='domcontentloaded')
                try:
                    await page.wait_for_selector('table', timeout=10000)
                except:
                    pass  # Continue even if timeout, might be loaded already

                links = await page.locator('a.screener-link-primary').all_inner_texts()
                if not links: links = await page.locator('a[href*="quote.ashx?t="]').all_inner_texts()

                for t in links:
                    t = t.strip()
                    if t.isalpha() and len(t) < 6: tickers.append(t)

                if tickers:
                    print(f"[DataScanner] Finviz Success: Found {len(tickers)} tickers.")
                    await browser.close()
                    return sorted(list(set(tickers)))
            except Exception as e:
                print(f"[DataScanner] Finviz failed ({e}), trying backup...")

            # 2. MarketWatch (Backup)
            if not tickers:
                try:
                    await page.goto(self.mw_url, timeout=30000)
                    rows = await page.locator('div.table__cell a.link').all_inner_texts()
                    for t in rows:
                        t = t.strip()
                        if t.isalpha() and len(t) < 6: tickers.append(t)
                except:
                    pass

            # 3. HighShortInterest (Last Resort)
            if not tickers:
                try:
                    await page.goto(self.hsi_url, timeout=20000)
                    tds = await page.locator('table.stocks tr td:nth-child(1)').all_inner_texts()
                    for t in tds:
                        t = t.strip()
                        if t.isalpha() and len(t) < 6: tickers.append(t)
                except:
                    pass

            await browser.close()

        unique = sorted(list(set(tickers)))
        if not unique:
            print("[DataScanner] ⚠️ All sources failed. Using manual fallback.")
            return ["GME", "AMC", "CVNA", "UPST", "MARA", "COIN", "MSTR", "BYND", "SPCE", "RIVN"]
        return unique


class SocialScanner:
    def __init__(self):
        # Use relative path for cookies or environment variable if needed
        self.cookies_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), COOKIES_PATH)

    async def check_social_sentiment(self, tickers):
        if not os.path.exists(self.cookies_path):
            print("[SocialScanner] No cookies found. Skipping X scan (Scores will be 0).")
            return {}

        sentiment_map = {}
        async with async_playwright() as p:
            args = ['--disable-blink-features=AutomationControlled', '--no-sandbox']
            # Headless must be True for CI/CD environments
            browser = await p.chromium.launch(headless=True, args=args)
            context = await browser.new_context(viewport={"width": 1280, "height": 800})

            try:
                with open(self.cookies_path, 'r') as f:
                    cookies = json.load(f)
                    clean_cookies = []
                    for c in cookies:
                        if "sameSite" in c and c["sameSite"] not in ["Strict", "Lax", "None"]: del c["sameSite"]
                        if "expirationDate" in c: c["expires"] = c.pop("expirationDate")
                        clean_cookies.append(c)
                    await context.add_cookies(clean_cookies)
            except Exception as e:
                print(f"[SocialScanner] Error loading cookies: {e}")
                await browser.close()
                return {}

            page = await context.new_page()
            print(f"[SocialScanner] Analyzing X Hype for top {len(tickers)} tickers...")

            for i, ticker in enumerate(tickers):
                print(f"\r   > [{i + 1}/{len(tickers)}] Checking ${ticker}...", end="", flush=True)
                try:
                    url = f"https://x.com/search?q=%24{ticker}%20squeeze&src=typed_query&f=live"
                    await page.goto(url, timeout=15000)
                    await asyncio.sleep(1.5)
                    content = await page.content()
                    if "No results for" in content:
                        sentiment_map[ticker] = 0
                    else:
                        cnt = await page.locator('article').count()
                        sentiment_map[ticker] = cnt
                except:
                    sentiment_map[ticker] = 0
            print("")
            await browser.close()
        return sentiment_map


class ShortSqueezePro:
    def __init__(self):
        # Base dir is the location of this script
        self.script_dir = os.path.dirname(os.path.abspath(__file__))

    def get_financial_data(self, ticker, reg_sho):
        yf_obj = yf.Ticker(ticker)
        res = {
            'valid': False, 'price': 0, 'mkt_cap_mm': 0,
            'short_float': 0, 'si_change_1m': 0, 'float_utilization': 0,
            'dtc': 0, 'price_momo_1m': 0, 'rsi': 0, 'vol_spike': "NO",
            'reg_sho': False, 'error': None
        }

        try:
            info = yf_obj.info
            # Some tickers might not have 'quoteType' or have different data structures
            if info.get('quoteType', 'EQUITY').upper() != 'EQUITY': return res

            mkt_cap = info.get('marketCap', 0)
            mkt_cap_mm = round(mkt_cap / 1_000_000, 2)
            if mkt_cap_mm < MIN_MKT_CAP_MM:
                res['error'] = f"Too Small (${mkt_cap_mm}M)"
                return res

            res['valid'] = True
            res['mkt_cap_mm'] = mkt_cap_mm
            res['reg_sho'] = reg_sho.is_on_list(ticker)
            res['short_float'] = round(info.get('shortPercentOfFloat', 0) * 100, 2)
            res['dtc'] = info.get('shortRatio', 0)

            shares_now = info.get('sharesShort', 0)
            shares_prev = info.get('sharesShortPriorMonth', 0)
            if shares_prev > 0:
                res['si_change_1m'] = round(((shares_now - shares_prev) / shares_prev) * 100, 2)

            float_shares = info.get('floatShares', 0)
            if float_shares > 0:
                res['float_utilization'] = round((float_shares - shares_now) / 1_000_000, 2)

            hist = yf_obj.history(period="2mo")
            if not hist.empty:
                curr_price = hist['Close'].iloc[-1]
                res['price'] = curr_price

                one_month_ago_idx = max(0, len(hist) - 22)
                price_1m_ago = hist['Close'].iloc[one_month_ago_idx]
                res['price_momo_1m'] = round(((curr_price - price_1m_ago) / price_1m_ago) * 100, 2)

                delta = hist['Close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(14).mean()

                # Handling division by zero for RSI
                avg_loss = loss.iloc[-1]
                if avg_loss == 0:
                    res['rsi'] = 100
                else:
                    rs = gain.iloc[-1] / avg_loss
                    res['rsi'] = round(100 - (100 / (1 + rs)), 2)

                avg_vol = hist['Volume'].iloc[-6:-1].mean()
                if avg_vol > 0:
                    curr_vol = hist['Volume'].iloc[-1]
                    if curr_vol > (avg_vol * 3): res['vol_spike'] = "YES"

        except Exception as e:
            res['error'] = str(e)
            res['valid'] = False

        return res

    def generate_html_report(self, df, filename):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')

        # New "Hype-Based" Signal
        def get_signal(row):
            score = row['Total_Score']
            hype = row['X_Mentions']

            # If Hype is extreme, it's always an ALERT
            if hype >= 10: return "🔥 VIRAL SQUEEZE"
            if score >= 80: return "🚀 SQUEEZE ALERT"
            if score >= 50: return "👀 WATCH"
            return "AVOID"

        df['Signal'] = df.apply(get_signal, axis=1)

        # --- SECTION 1: TOP SQUEEZE OPPORTUNITIES (Cards) ---
        top_picks = df[df['Signal'].str.contains("SQUEEZE")].head(3)
        if top_picks.empty: top_picks = df.head(3)

        cards_html = ""
        for _, row in top_picks.iterrows():
            cards_html += f"""
            <div class="card">
                <div class="card-header">
                    <span class="ticker">${row['Ticker']}</span>
                    <span class="price">${row['Price']:.2f}</span>
                </div>
                <div class="card-body">
                    <div class="metric-row"><span>Total Score:</span><span class="highlight-blue" style="font-size:18px">{int(row['Total_Score'])}</span></div>
                    <div class="metric-row"><span>X Mentions:</span><span class="highlight-red" style="font-size:18px">🔥 {row['X_Mentions']}</span></div>
                    <div class="metric-row"><span>Short Float:</span><span>{row['Short_Float%']}%</span></div>
                    <div class="action-badge">{row['Signal']}</div>
                </div>
            </div>"""

        # --- SECTION 2: FULL MARKET SCAN (Table) ---
        table_rows = ""
        rank = 1
        for _, row in df.iterrows():
            sig_col = "#00E396" if "SQUEEZE" in row['Signal'] else "#FEB019" if "WATCH" in row['Signal'] else "#FF4560"
            sho_badge = "✅ YES" if row['RegSHO'] else "-"
            sho_style = "color:#FF4560; font-weight:bold" if row['RegSHO'] else "color:#64748b"
            vol_style = "color:#FEB019; font-weight:bold" if row['Vol_Spike'] == "YES" else ""

            # Hype highlighting
            hype_style = "color:#fff"
            if row['X_Mentions'] >= 5: hype_style = "color:#00E396; font-weight:bold; font-size:15px"
            if row['X_Mentions'] >= 10: hype_style = "color:#FF4560; font-weight:bold; font-size:16px"

            x_link = f"https://x.com/search?q=%24{row['Ticker']}%20squeeze"

            table_rows += f"""
            <tr>
                <td>{rank}</td>
                <td><strong><a href="{x_link}" target="_blank" style="color:#fff;text-decoration:none">${row['Ticker']}</a></strong></td>
                <td>${row['Price']:.2f}</td>
                <td style="color:{sig_col}; font-weight:bold;">{row['Signal']}</td>
                <td style="background:rgba(255,255,255,0.05); font-weight:bold; color:#00E396">{int(row['Total_Score'])}</td>
                <td style="{hype_style}">{row['X_Mentions']}</td>
                <td style="{sho_style}">{sho_badge}</td>
                <td>{row['Short_Float%']}%</td>
                <td>{row['RSI']}</td>
                <td style="{vol_style}">{row['Vol_Spike']}</td>
                <td>{row['Days_to_Cover']}</td>
            </tr>"""
            rank += 1

        html = f"""<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>ParisTrader - Retail Hype Scanner</title>
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
                body {{ background:#0f172a; color:#f1f5f9; font-family:'Inter', sans-serif; padding:20px; }}
                .container {{ max_width:1400px; margin:0 auto; }}
                header {{ display:flex; justify-content:space-between; margin-bottom:20px; border-bottom:1px solid #334155; padding-bottom:10px; }}
                .brand {{ font-size:24px; font-weight:700; }} .brand span {{ color:#00E396; }}

                .cards-container {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 40px; }}
                .card {{ background: #1e293b; border-radius: 12px; padding: 20px; border: 1px solid #334155; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }}
                .card-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; }}
                .ticker {{ font-size: 28px; font-weight: 700; color: #fff; }}
                .price {{ font-size: 20px; color: #00E396; }}
                .metric-row {{ display: flex; justify-content: space-between; margin-bottom: 8px; font-size: 14px; color: #cbd5e1; }}
                .highlight-red {{ color: #FF4560; font-weight: bold; }}
                .highlight-blue {{ color: #008FFB; font-weight: bold; }}
                .action-badge {{ background: rgba(0, 227, 150, 0.15); color: #00E396; text-align: center; padding: 8px; border-radius: 6px; font-weight: bold; margin-top: 15px; border: 1px solid rgba(0, 227, 150, 0.3); }}

                table {{ width:100%; border-collapse:collapse; background:#1e293b; border-radius:8px; overflow:hidden; font-size:14px; }}
                th {{ background:#0f172a; padding:12px; text-align:left; color:#94a3b8; font-size:12px; text-transform:uppercase; }}
                td {{ padding:12px; border-bottom:1px solid #334155; }}
                tr:hover {{ background:#334155; cursor:pointer; }}

                h2 {{ color: #fff; margin-bottom: 20px; font-size: 20px; border-left: 4px solid #00E396; padding-left: 10px; }}
                .legend {{ margin-top:20px; font-size:12px; color:#64748b; text-align:center; }}
            </style>
        </head>
        <body>
            <div class="container">
                <header>
                    <div class="brand">Short Squeeze <span>LEADERBOARD</span></div>
                    <div>{timestamp}</div>
                </header>

                <h2>🔥 Top Retail Wisdom Picks (Hype Weighted)</h2>
                <div class="cards-container">{cards_html}</div>

                <h2>📊 Full Market Scan (wait for the signal, check every day)</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Rank</th><th>Symbol</th><th>Price</th><th>Signal</th>
                            <th>Total Score</th><th>X Hype</th>
                            <th>Reg SHO</th><th>Short Float</th><th>RSI</th><th>Vol Spike</th><th>DTC</th>
                        </tr>
                    </thead>
                    <tbody>{table_rows}</tbody>
                </table>
                <div class="legend">
                    <strong>Total Score:</strong> Fundamentals + (X Mentions * 2). Retail Hype is the primary driver. <br>
                    <strong>X Hype:</strong> Volume of recent "squeeze" tweets.
                </div>
            </div>
        </body></html>"""

        with open(filename, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"HTML Report: {filename}")

    def run(self):
        print(f"[{datetime.now():%H:%M:%S}] Starting Hype-First Scanner...")

        reg_sho = RegSHO()
        data_scanner = DataScanner()
        social_scanner = SocialScanner()

        # Step 1: Source Candidates (Data First)
        try:
            tickers = asyncio.run(data_scanner.get_most_shorted_stocks())
        except Exception as e:
            print(f"Error fetching tickers: {e}")
            tickers = []

        manual_adds = ["GME", "AMC", "CVNA", "UPST", "MSTR", "COIN", "MARA"]
        tickers = list(set(tickers + manual_adds))

        results = []
        print(f"\nAnalyzing fundamentals for {len(tickers)} tickers...")

        # Step 2: Scoring Fundamentals
        for i, ticker in enumerate(tickers):
            print(f"\rProcessing {i + 1}/{len(tickers)}: {ticker}...", end="", flush=True)
            data = self.get_financial_data(ticker, reg_sho)

            if not data['valid']: continue

            # BASE SCORE (Fundamentals)
            base_score = 0
            base_score += min(40, data['short_float'])
            base_score += min(20, data['dtc'] * 2)
            if data['reg_sho']: base_score += 20
            if data['price_momo_1m'] > 20: base_score += 10
            if data['vol_spike'] == "YES": base_score += 10

            results.append({
                "Ticker": ticker, "Base_Score": base_score, "Price": data['price'],
                "RegSHO": data['reg_sho'], "Short_Float%": data['short_float'],
                "SI_Chg_1M": data['si_change_1m'], "Days_to_Cover": data['dtc'],
                "Mkt_Cap_MM": data['mkt_cap_mm'], "Float_Unshorted_MM": data['float_utilization'],
                "Price_Momo_1M": data['price_momo_1m'], "RSI": data['rsi'],
                "Vol_Spike": data['vol_spike']
            })

        # Step 3: SOCIAL HYPE BOOST (Re-Ranking)
        # We check MORE candidates now (Top 50) to catch hidden viral stocks
        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values(by='Base_Score', ascending=False)
            candidates_to_check = df.head(TOP_X_SCAN_LIMIT)['Ticker'].tolist()

            print(f"\n\nChecking Retail Wisdom (X Hype) for Top {len(candidates_to_check)} candidates...")
            try:
                hype_data = asyncio.run(social_scanner.check_social_sentiment(candidates_to_check))
            except Exception as e:
                print(f"Error checking sentiment: {e}")
                hype_data = {}

            df['X_Mentions'] = df['Ticker'].map(hype_data).fillna(0).astype(int)

            # --- THE NEW SCORING FORMULA ---
            # Total Score = Base Score + (X Mentions * 2)
            # Retail Wisdom is the heaviest weight. 50 mentions = +100 points.
            df['Total_Score'] = df['Base_Score'] + (df['X_Mentions'] * 2)

            # Sort by TOTAL SCORE (Hype Driven)
            df = df.sort_values(by='Total_Score', ascending=False)

            # Output
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            # Save to the same directory as the script
            # excel_path = os.path.join(self.script_dir, f"Short_squeeze_{timestamp}.xlsx")
            html_path = os.path.join(self.script_dir, f"Short_squeeze_{timestamp}.html")

            # df.to_excel(excel_path, index=False) # Skip excel for this workflow
            self.generate_html_report(df, html_path)

            print("\n--- TOP LEADERBOARD (Hype Weighted) ---")
            print(df[['Ticker', 'Total_Score', 'X_Mentions', 'Short_Float%', 'Base_Score']].head(5).to_string(
                index=False))
        else:
            print("\nNo valid data found.")

        print(f"[{datetime.now():%H:%M:%S}] Done.")


if __name__ == "__main__":
    app = ShortSqueezePro()
    app.run()