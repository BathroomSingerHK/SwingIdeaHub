import pandas as pd
import yfinance as yf
import statsmodels.api as sm
from statsmodels.regression.rolling import RollingOLS
import requests
import zipfile
import io
import numpy as np
import os
from datetime import datetime, timedelta

# ==========================================
# 1. Configuration & Setup
# ==========================================

OUTPUT_FILE = "stock_factor_data.csv"
OUTPUT_RETURNS_FILE = "stock_returns_data.csv"  # For Correlation Matrix
INPUT_FILE = "stock_list.csv"


def load_stock_list():
    """Reads stock list from stock_list.csv in the same directory."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, INPUT_FILE)

    print(f"Reading stock list: {file_path}")

    if not os.path.exists(file_path):
        print(f"Error: {INPUT_FILE} not found. Please ensure it exists.")
        return []

    try:
        df = pd.read_csv(file_path, header=0, usecols=[0], dtype=str)
        tickers = df.iloc[:, 0].dropna().astype(str).str.strip().str.upper().tolist()
        valid_tickers = [t for t in tickers if t and t.lower() != 'nan']
        print(f"Loaded {len(valid_tickers)} tickers.")
        return valid_tickers
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []


# ==========================================
# 2. Main Processing Logic
# ==========================================
def process_stocks():
    tickers = load_stock_list()
    if not tickers: return

    # --- 1. Download Fama-French 5 Factors Data ---
    print("Downloading Fama-French 5 Factors data...")
    url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip"

    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers)
        r.raise_for_status()

        z = zipfile.ZipFile(io.BytesIO(r.content))
        file_list = z.namelist()
        csv_files = [x for x in file_list if x.lower().endswith('.csv')]

        if not csv_files:
            print(f"Error: No CSV file found in ZIP. Contents: {file_list}")
            return

        f_name = csv_files[0]
        print(f"Using Fama-French file: {f_name}")

        ff_data = pd.read_csv(z.open(f_name), skiprows=3, index_col=0)
        ff_data.index = pd.to_datetime(ff_data.index, format='%Y%m%d', errors='coerce')
        ff_data = ff_data.dropna()
        ff_data = ff_data / 100.0
        print("Fama-French data ready.")

    except Exception as e:
        print(f"Failed to download/process Fama-French data: {e}")
        return

    results = []
    all_returns_data = {}

    print(f"Starting analysis for {len(tickers)} stocks...")

    for i, ticker in enumerate(tickers):
        try:
            print(f"[{i + 1}/{len(tickers)}] Processing {ticker}...", end=" ")

            # --- 2. Download Stock Data ---
            df = yf.download(ticker, period="18mo", progress=False, auto_adjust=False)

            if df.empty or len(df) < 252:
                print("Skipped (Insufficient Data)")
                continue

            # Handle MultiIndex columns
            if isinstance(df.columns, pd.MultiIndex):
                if ticker in df.columns.levels[0]:
                    df = df.xs(ticker, axis=1, level=0)
                elif len(df.columns.levels) > 1 and ticker in df.columns.levels[1]:
                    df = df.xs(ticker, axis=1, level=1)

                if isinstance(df.columns, pd.MultiIndex):
                    try:
                        df = df['Close']
                        if isinstance(df, pd.DataFrame):
                            df = df.iloc[:, 0].to_frame('Close')
                        else:
                            df = df.to_frame('Close')
                    except:
                        pass

            if 'Close' not in df.columns:
                if isinstance(df, pd.Series):
                    df = df.to_frame('Close')
                else:
                    df = df.iloc[:, 0].to_frame('Close')

            # --- 3. Save Returns for Correlation Matrix ---
            daily_returns = df['Close'].pct_change().dropna()

            if len(daily_returns) == 0:
                print("Skipped (No Returns)")
                continue

            all_returns_data[ticker] = daily_returns.tail(252)

            # --- 4. Prepare Data for Factor Analysis ---
            df_merged = pd.DataFrame({'Stock_Ret': daily_returns}).join(ff_data, how='inner').dropna()

            if len(df_merged) < 126:
                print("Skipped (Not enough overlapping data)")
                continue

            df_merged['Ex_Ret'] = df_merged['Stock_Ret'] - df_merged['RF']

            exog_vars = ['Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA']
            X = sm.add_constant(df_merged[exog_vars])
            y = df_merged['Ex_Ret']

            model = RollingOLS(y, X, window=252)
            rolling_res = model.fit()
            betas = rolling_res.params.iloc[-1]

            if betas.isna().any():
                print("Skipped (NaN in regression)")
                continue

            # --- 5. Scoring Logic (0-100) [FIXED] ---
            # Added min/max clamping to ALL scores to prevent negative values
            baskets = []

            # Beta (Market Risk)
            beta_mkt = betas['Mkt-RF']
            score_beta = min(max((beta_mkt * 50), 0), 100)
            if beta_mkt > 1.3:
                baskets.append("Aggressive High Beta")
            elif beta_mkt < 0.7:
                baskets.append("Defensive Low Vol")

            # Size (SMB)
            beta_smb = betas['SMB']
            score_size = min(max(50 + (beta_smb * 50), 0), 100)  # [FIXED] Added min/max
            if beta_smb > 0.5:
                baskets.append("Small Cap Tilt")
            elif beta_smb < -0.5:
                baskets.append("Large Cap Tilt")

            # Value (HML)
            beta_hml = betas['HML']
            score_value = min(max(50 + (beta_hml * 50), 0), 100)  # [FIXED] Added min/max
            if beta_hml > 0.3:
                baskets.append("Deep Value")
            elif beta_hml < -0.3:
                baskets.append("High Growth")

            # Quality (RMW)
            beta_rmw = betas['RMW']
            score_quality = min(max(50 + (beta_rmw * 100), 0), 100)  # [FIXED] Added min/max
            if beta_rmw > 0.3: baskets.append("High Quality MOAT")

            # Momentum
            px = df['Close']
            mom_raw = (px.iloc[-1] / px.iloc[-126]) - 1 if len(px) > 126 else 0
            score_mom = min(max(50 + (mom_raw * 100), 0), 100)
            if mom_raw > 0.2:
                baskets.append("Momentum Leader")
            elif mom_raw < -0.1:
                baskets.append("Falling Knife")

            baskets_str = "; ".join(baskets) if baskets else "Neutral"

            beta_trend = rolling_res.params['Mkt-RF'].dropna().iloc[::20].tail(10).values
            beta_trend_str = ",".join([f"{x:.2f}" for x in beta_trend])

            row = {
                'Ticker': ticker,
                'Last_Date': df_merged.index[-1].strftime('%Y-%m-%d'),
                'Score_Beta': round(score_beta, 2),
                'Score_Size': round(score_size, 2),
                'Score_Value': round(score_value, 2),
                'Score_Mom': round(score_mom, 2),
                'Score_Quality': round(score_quality, 2),
                'Beta_Raw': round(beta_mkt, 3),
                'SMB_Raw': round(beta_smb, 3),
                'HML_Raw': round(beta_hml, 3),
                'RMW_Raw': round(beta_rmw, 3),
                'Baskets': baskets_str,
                'Beta_Trend': beta_trend_str
            }
            results.append(row)
            print("Done.")

        except Exception as e:
            print(f"Failed ({e})")

    # --- 6. Save Files ---
    if results:
        output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_FILE)
        df_out = pd.DataFrame(results)
        df_out.to_csv(output_path, index=False)
        print(f"Factor DNA Analysis saved to: {output_path}")
    else:
        print("No results to save.")

    if all_returns_data:
        returns_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_RETURNS_FILE)
        print("Saving Returns Data...")
        returns_df = pd.DataFrame(all_returns_data)
        returns_df.index = pd.to_datetime(returns_df.index).strftime('%Y-%m-%d')
        returns_df.to_csv(returns_path, index_label='Date')
        print(f"Historical Returns saved to: {returns_path}")


if __name__ == "__main__":
    process_stocks()