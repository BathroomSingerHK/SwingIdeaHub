import pandas as pd
import numpy as np
import yfinance as yf

# Your open positions (update prices if needed for latest)
positions = {
    'GOOGL': {'direction': 'LONG', 'shares': 500, 'price': 313.51},
    'RKLB': {'direction': 'LONG', 'shares': 1500, 'price': 70.65},
    'AMC': {'direction': 'SHORT', 'shares': 53000, 'price': 1.69},
    'ALB': {'direction': 'LONG', 'shares': 500, 'price': 150.01},
    'RTX': {'direction': 'LONG', 'shares': 500, 'price': 185.17},
    'STT': {'direction': 'LONG', 'shares': 600, 'price': 132.28},
    'AVGO': {'direction': 'LONG', 'shares': 250, 'price': 352.13},
    'SOFI': {'direction': 'LONG', 'shares': 2000, 'price': 27.07},
}

# Calculate values and weights
df = pd.DataFrame.from_dict(positions, orient='index')
df['value'] = df['shares'] * df['price']
df['signed_value'] = np.where(df['direction'] == 'LONG', df['value'], -df['value'])
df['abs_value'] = df['value'].abs()
total_abs = df['abs_value'].sum()
df['weight'] = df['abs_value'] / total_abs

tickers = list(positions.keys())

# Download data with explicit auto_adjust=False
data = yf.download(tickers, period="2y", auto_adjust=False)['Adj Close']

# Calculate daily returns
returns = data.pct_change().dropna()

# Use last ~252 trading days (1 year)
returns = returns.tail(252)

print("Downloaded tickers:", returns.columns.tolist())
print(f"Absolute Exposure (for VaR base): ${total_abs:,.0f}")

# Weights in same order as tickers
weights = df['weight'].values

# Historical Simulation VaR
port_returns_hist = returns.dot(weights)
var_95_hist = -np.percentile(port_returns_hist, 5)  # Positive loss
var_99_hist = -np.percentile(port_returns_hist, 1)

print(f"\nHistorical 1-Day 95% VaR: ${var_95_hist * total_abs:,.0f} ({var_95_hist*100:.2f}%)")
print(f"Historical 1-Day 99% VaR: ${var_99_hist * total_abs:,.0f} ({var_99_hist*100:.2f}%)")

# Parametric VaR (assumes normality)
daily_vol = port_returns_hist.std()
var_95_param = 1.65 * daily_vol * total_abs
var_99_param = 2.33 * daily_vol * total_abs

print(f"\nDaily Portfolio Volatility: {daily_vol*100:.2f}%")
print(f"Parametric 1-Day 95% VaR: ${var_95_param:,.0f}")
print(f"Parametric 1-Day 99% VaR: ${var_99_param:,.0f}")