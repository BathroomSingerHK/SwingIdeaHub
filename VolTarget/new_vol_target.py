import yfinance as yf
import pandas as pd
import json
import os
import time
from datetime import datetime, timedelta

# --- 設定基礎路徑 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "stock_list.csv")
OUTPUT_HTML = os.path.join(BASE_DIR, "vol_tool.html")


# --- 1. DATA FETCHING FUNCTION ---
def fetch_market_data():
    """
    Reads tickers, downloads 2y history in batches, and returns a clean JSON string.
    """
    print("--- Starting Data Download ---")

    # 1. Get Tickers
    if not os.path.exists(INPUT_FILE):
        print(f"Warning: {INPUT_FILE} not found. Using default list.")
        tickers = ['SPY', 'QQQ', 'IWM', 'AAPL', 'MSFT', 'NVDA', 'TSLA', 'GOOGL', 'AMZN']
    else:
        try:
            df = pd.read_csv(INPUT_FILE)
            # 讀取第一欄，去除空白，轉大寫
            raw_tickers = df.iloc[:, 0].astype(str).str.strip().str.upper().tolist()
            # 過濾無效值並修正符號 (例如 BRK/A -> BRK-A)
            tickers = [t.replace('/', '-') for t in raw_tickers if t and t != 'NAN']
            # 去除重複
            tickers = list(set(tickers))
        except Exception as e:
            print(f"Error reading csv: {e}")
            return None

    print(f"Total tickers to fetch: {len(tickers)}")

    # 2. Download Data in Batches (避免 Timeout)
    BATCH_SIZE = 20
    all_data_frames = []

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i: i + BATCH_SIZE]
        print(f"Downloading batch {i // BATCH_SIZE + 1}: {batch}")

        try:
            # 下載該批次
            data = yf.download(
                batch,
                period="2y",
                group_by='ticker',
                auto_adjust=True,
                progress=False,
                threads=True
            )

            if data.empty:
                print("  -> Batch returned empty.")
                continue

            # 提取 Close 價格
            # 情況 A: 單一股票 (Columns 不是 MultiIndex 或只有一層)
            if len(batch) == 1:
                # yfinance 單一股票下載時結構較簡單
                if 'Close' in data.columns:
                    df_close = data[['Close']].rename(columns={'Close': batch[0]})
                    all_data_frames.append(df_close)

            # 情況 B: 多檔股票 (MultiIndex: Ticker -> Price 或 Price -> Ticker)
            else:
                # 嘗試標準提取
                try:
                    # 如果 columns 是 (Price, Ticker)，我們取 'Close'
                    # yfinance 最近版本通常是 (Price, Ticker) 結構
                    # 但有時 group_by='ticker' 會變成 (Ticker, Price)
                    # 最穩健的方法是檢查層級

                    if isinstance(data.columns, pd.MultiIndex):
                        # 檢查 'Close' 在哪一層
                        if 'Close' in data.columns.get_level_values(0):
                            # 結構: Close -> Ticker
                            df_close = data['Close']
                        elif 'Close' in data.columns.get_level_values(1):
                            # 結構: Ticker -> Close
                            df_close = data.xs('Close', level=1, axis=1)
                        else:
                            print("  -> 'Close' column not found in MultiIndex.")
                            continue
                    else:
                        # 只有一層 columns (極少見，除非只下載成功一檔)
                        if 'Close' in data.columns:
                            df_close = data[['Close']]
                        else:
                            continue

                    all_data_frames.append(df_close)

                except Exception as e:
                    print(f"  -> Error extracting data from batch: {e}")

        except Exception as e:
            print(f"  -> Batch download failed: {e}")

        # 禮貌性延遲，避免被封鎖 IP
        time.sleep(1)

    if not all_data_frames:
        print("Error: No data fetched successfully.")
        return None

    # 3. 合併所有批次資料
    print("Merging data...")
    try:
        final_df = pd.concat(all_data_frames, axis=1)
        # 刪除重複的欄位 (以防萬一)
        final_df = final_df.loc[:, ~final_df.columns.duplicated()]
    except Exception as e:
        print(f"Merge failed: {e}")
        return None

    # 4. 清理資料
    final_df.dropna(how='all', inplace=True)  # 刪除完全沒資料的日期
    final_df = final_df.round(2)

    # 處理索引
    final_df.index.name = 'Date'
    final_df.reset_index(inplace=True)
    final_df['Date'] = final_df['Date'].dt.strftime('%Y-%m-%d')

    # 轉為 JSON
    json_data = final_df.to_json(orient='records')

    print(f"--- Data Download Complete. Rows: {len(final_df)}, Columns: {len(final_df.columns)} ---")
    return json_data


# --- 2. HTML GENERATION ---
def generate_html(json_data):
    html_template = """
<!DOCTYPE html>
<html lang="zh-HK">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Vol Target Calculator</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>

    <style>
        body { background-color: #f8f9fa; padding-top: 40px; padding-bottom: 60px; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        .card { border: none; box-shadow: 0 4px 15px rgba(0,0,0,0.05); border-radius: 12px; margin-bottom: 30px; }
        .stat-label { font-size: 0.85rem; color: #6c757d; text-transform: uppercase; letter-spacing: 1px; font-weight: 600; }
        .stat-value { font-size: 1.5rem; font-weight: 700; color: #212529; }
        .highlight-blue { color: #0d6efd; }
        .edu-section h5 { font-weight: 700; color: #495057; margin-bottom: 15px; }
        .edu-section p { color: #6c757d; font-size: 0.95rem; line-height: 1.6; }
        .table-custom th { background-color: #e9ecef; font-size: 0.9rem; }
        .table-custom td { font-size: 0.9rem; }
        .disclaimer { font-size: 0.75rem; color: #adb5bd; margin-top: 50px; text-align: justify; border-top: 1px solid #dee2e6; padding-top: 20px;}
        .hidden { display: none; }
    </style>
</head>
<body>

<div class="container">
    <div class="row justify-content-center">
        <div class="col-lg-8">

            <div class="text-center mb-5">
                <h2 class="fw-bold">Volatility Target Calculator</h2>
                <p class="text-muted">波動率部位計算機 (學術研究用途 | Data Last Updated: <span id="updateDate"></span>)</p>
            </div>

            <div class="card p-4">
                <div class="row g-3">
                    <div class="col-md-6">
                        <label class="form-label">總資金 (USD)</label>
                        <input type="number" id="capital" class="form-control" value="100000">
                    </div>
                    <div class="col-md-6">
                        <label class="form-label">股票代號 (Ticker)</label>
                        <input type="text" id="ticker" class="form-control" value="SPY" placeholder="e.g. SPY">
                        <div class="form-text" id="tickerHint">Checking database...</div>
                    </div>
                    <div class="col-md-6">
                        <label class="form-label">目標波動率 (Target Vol %)</label>
                        <input type="number" id="target_vol" step="0.1" class="form-control" value="15">
                        <div class="form-text">進取: 20% | 平衡: 15% | 保守: 10%</div>
                    </div>
                    <div class="col-md-6">
                        <label class="form-label">回測週期 (Lookback)</label>
                        <select id="lookback" class="form-select">
                            <option value="20">20 日 (靈敏/短期)</option>
                            <option value="60">60 日 (中期)</option>
                            <option value="252">252 日 (長期趨勢)</option>
                        </select>
                    </div>
                    <div class="col-12 mt-4">
                        <button onclick="calculate()" class="btn btn-primary w-100 py-2 fw-bold">生成計算結果 (Calculate)</button>
                    </div>
                </div>
            </div>

            <div id="errorBox" class="alert alert-danger shadow-sm border-0 hidden"></div>

            <div id="resultCard" class="card p-4 animate-in hidden">
                <div class="d-flex justify-content-between align-items-center mb-3">
                    <h3 id="resTicker" class="m-0 text-primary fw-bold"></h3>
                    <span id="resLookback" class="badge bg-light text-dark border"></span>
                </div>

                <hr class="text-muted opacity-25">

                <div class="row mb-4">
                    <div class="col-6">
                        <div class="stat-label">參考市價</div>
                        <div id="resPrice" class="stat-value"></div>
                    </div>
                    <div class="col-6 text-end">
                        <div class="stat-label">歷史波動率 (Realized)</div>
                        <div id="resVol" class="stat-value highlight-blue"></div>
                    </div>
                </div>

                <div class="row mb-4">
                     <div class="col-6">
                        <div class="stat-label">目標權重曝險 (Target Exposure)</div>
                        <div id="resExposure" class="stat-value"></div>
                        <small id="resLeverage" class="text-muted"></small>
                    </div>
                    <div class="col-6 text-end">
                        <div class="stat-label">理論持倉股數</div>
                        <div id="resShares" class="stat-value text-success"></div>
                    </div>
                </div>

                <div id="alertMargin" class="alert alert-warning border-warning d-flex align-items-center hidden">
                    <span class="me-2">📊</span>
                    <div>
                        <strong>權重分析 (Leverage Required):</strong> 
                        根據公式計算，由於當前波動率 (<span id="txtVolMargin"></span>%) 低於設定參數，模型顯示需配置 <span id="txtLevMargin"></span> 倍權重以符合目標風險。
                    </div>
                </div>

                <div id="alertCash" class="alert alert-success border-success d-flex align-items-center hidden">
                    <span class="me-2">📊</span>
                    <div>
                        <strong>權重分析 (Cash Only):</strong> 
                        根據公式計算，模型顯示僅需使用 <span id="txtCashPct"></span>% 資金即可符合目標風險參數。
                    </div>
                </div>

                <div id="volChart" class="mt-3"></div>
            </div>

            <div class="card p-5 bg-white edu-section">
                <h4 class="mb-4 pb-2 border-bottom">📚 關於 Volatility Targeting 模型</h4>

                <div class="row g-5">
                    <div class="col-md-6">
                        <h5>1. 模型概念</h5>
                        <p>本工具僅演示「目標波動率」之數學模型。不同於固定倉位（如 100% 持股），此數學模型旨在計算如何透過動態調整部位，使投資組合維持在使用者設定的風險數值上。</p>
                        <p class="text-muted small">計算公式：(目標波動率 / 歷史波動率) × 本金</p>
                    </div>

                    <div class="col-md-6">
                        <h5>2. 機制說明</h5>
                        <ul class="text-muted small mb-0 ps-3">
                            <li class="mb-2"><strong>低波動環境:</strong> 當分母 (歷史 Vol) 變小，公式導出的理論權重會增加。</li>
                            <li><strong>高波動環境:</strong> 當分母 (歷史 Vol) 變大，公式導出的理論權重會減少。</li>
                        </ul>
                    </div>

                    <div class="col-md-12">
                        <div class="p-3 bg-light rounded border">
                            <h5 class="text-primary">3. 數據參考：16法則 (The Rule of 16)</h5>
                            <p>金融學術上常用 16法則來將年化波動率換算為日波動率。</p>

                            <table class="table table-sm table-custom table-bordered text-center w-75 mx-auto bg-white">
                                <thead>
                                    <tr>
                                        <th>年化波動率 (Annual Vol)</th>
                                        <th>≈ 理論單日波動 (Implied Daily Move)</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td>8%</td>
                                        <td>0.5%</td>
                                    </tr>
                                    <tr class="table-primary fw-bold">
                                        <td>16%</td>
                                        <td>1.0%</td>
                                    </tr>
                                    <tr>
                                        <td>32%</td>
                                        <td>2.0%</td>
                                    </tr>
                                    <tr>
                                        <td>48%</td>
                                        <td>3.0%</td>
                                    </tr>
                                </tbody>
                            </table>
                            <p class="small text-muted text-center mt-2 mb-0">註：此換算僅供學術參考，不代表未來實際走勢。</p>
                        </div>
                    </div>
                </div>
            </div>

            <div class="disclaimer">
                <p><strong>免責聲明 (Disclaimer):</strong></p>
                <p>本網頁及其生成的內容僅供<strong>教育、學術研究及資訊參考用途</strong>，並非及不應被視為邀請、要約、招攬或建議買賣任何投資產品。本工具僅為一個數學計算機，其結果完全取決於使用者自行輸入的參數及歷史公開數據。</p>
                <p>本工具不構成任何形式的投資意見或專業建議 (包括但不限於財務、法律或稅務建議)。使用者不應依賴本工具的計算結果作為投資決策的唯一依據。投資涉及風險，證券價格可升可跌，過往表現不代表將來表現。如需投資建議，請諮詢持牌專業財務顧問。</p>
                <p>&copy; 2026 BathroomQuant Group</p>
            </div>

        </div>
    </div>
</div>

<script>
    // --- DATA INJECTION POINT ---
    const marketData = {{DATA_INJECTION}};

    window.onload = function() {
        if(marketData && marketData.length > 0) {
            const lastRow = marketData[marketData.length - 1];
            document.getElementById('updateDate').innerText = lastRow.Date;
            document.getElementById('tickerHint').innerText = "Database loaded. Ready to calculate.";
        }
    };

    function calculateReturns(prices) {
        let returns = [];
        for (let i = 1; i < prices.length; i++) {
            let p_t = prices[i];
            let p_prev = prices[i-1];
            if (p_t > 0 && p_prev > 0) {
                returns.push(Math.log(p_t / p_prev));
            } else {
                returns.push(0); 
            }
        }
        return returns;
    }

    function calculateRollingVol(returns, window) {
        let vols = [];
        const sqrt252 = Math.sqrt(252);

        for (let i = 0; i < returns.length; i++) {
            if (i < window - 1) {
                vols.push(null); 
                continue;
            }
            let slice = returns.slice(i - window + 1, i + 1);
            let mean = slice.reduce((a, b) => a + b, 0) / slice.length;
            let variance = slice.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / (slice.length - 1);
            let stdDev = Math.sqrt(variance);
            vols.push(stdDev * sqrt252);
        }
        return vols;
    }

    function calculate() {
        document.getElementById('errorBox').classList.add('hidden');
        document.getElementById('resultCard').classList.add('hidden');

        const capital = parseFloat(document.getElementById('capital').value);
        const ticker = document.getElementById('ticker').value.trim().toUpperCase();
        const targetVol = parseFloat(document.getElementById('target_vol').value) / 100.0;
        const lookback = parseInt(document.getElementById('lookback').value);

        let dates = [];
        let prices = [];

        if (marketData.length === 0) return;

        let tickerKey = Object.keys(marketData[0]).find(k => k.toUpperCase() === ticker);

        if (!tickerKey) {
            showError(`Ticker '${ticker}' not found in database. Available tickers: ` + Object.keys(marketData[0]).filter(k => k !== 'Date').join(', '));
            return;
        }

        for (let row of marketData) {
            if (row[tickerKey] != null) {
                dates.push(row.Date);
                prices.push(row[tickerKey]);
            }
        }

        if (prices.length < lookback + 2) {
            showError(`Not enough data for ${ticker}. Found ${prices.length} days.`);
            return;
        }

        const returns = calculateReturns(prices);
        const volSeries = calculateRollingVol(returns, lookback);

        const currentPrice = prices[prices.length - 1];
        const currentVol = volSeries[volSeries.length - 1];

        if (currentVol == null || isNaN(currentVol)) {
            showError("Could not calculate volatility.");
            return;
        }

        const leverage = targetVol / currentVol;
        const exposure = capital * leverage;
        const shares = Math.floor(exposure / currentPrice);

        document.getElementById('resTicker').innerText = ticker;
        document.getElementById('resLookback').innerText = `Lookback: ${lookback} Days`;
        document.getElementById('resPrice').innerText = "$" + currentPrice.toLocaleString(undefined, {minimumFractionDigits: 2});
        document.getElementById('resVol').innerText = (currentVol * 100).toFixed(2) + "%";
        document.getElementById('resExposure').innerText = "$" + exposure.toLocaleString(undefined, {maximumFractionDigits: 2});
        document.getElementById('resLeverage').innerText = `模型權重: ${leverage.toFixed(2)}x`;
        document.getElementById('resShares').innerText = shares + " 股";

        document.getElementById('alertMargin').classList.add('hidden');
        document.getElementById('alertCash').classList.add('hidden');

        if (leverage > 1.0) {
            document.getElementById('alertMargin').classList.remove('hidden');
            document.getElementById('txtVolMargin').innerText = (currentVol * 100).toFixed(1);
            document.getElementById('txtLevMargin').innerText = leverage.toFixed(2);
        } else {
            document.getElementById('alertCash').classList.remove('hidden');
            document.getElementById('txtCashPct').innerText = (leverage * 100).toFixed(1);
        }

        const chartDates = dates.slice(1); 
        const trace = {
            x: chartDates,
            y: volSeries,
            type: 'scatter',
            mode: 'lines',
            name: 'Volatility',
            line: {color: '#0d6efd', width: 2}
        };

        const layout = {
            title: {text: `${ticker} Historical Volatility`, x: 0.01},
            yaxis: {title: 'Annualized Vol', tickformat: '.0%'},
            xaxis: {showgrid: false},
            template: 'plotly_white',
            height: 300,
            margin: {l: 40, r: 20, t: 50, b: 20}
        };

        Plotly.newPlot('volChart', [trace], layout, {displayModeBar: false});
        document.getElementById('resultCard').classList.remove('hidden');
    }

    function showError(msg) {
        const errBox = document.getElementById('errorBox');
        errBox.innerText = msg;
        errBox.classList.remove('hidden');
    }
</script>

</body>
</html>
    """

    final_html = html_template.replace("{{DATA_INJECTION}}", json_data)

    # [關鍵] 使用時間戳記檔名
    file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"vol_tool_{file_timestamp}.html"
    output_path = os.path.join(BASE_DIR, output_filename)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_html)

    print(f"--- Success! Generated {output_path} ---")


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    data_json = fetch_market_data()
    if data_json:
        generate_html(data_json)