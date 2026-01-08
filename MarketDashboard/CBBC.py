# -*- coding: utf-8 -*-
"""
CBBC HSI Notional 計算器 - V10 (最終欄位命名修正版)
功能：
1. 下載 HKEX CBBC 全表。
2. 計算牛熊重貨區 (Ladder) 與 詳細價位分佈 (Price Detail)。
3. 自動抓取 USD/HKD 匯率。
4. 輸出固定檔名的 HTML，並將關鍵欄位統一重命名為 "Knock-out notional($mio)"。
"""

import pandas as pd
import datetime as dt
import os
import requests
import yfinance as yf
import warnings
import time
from io import StringIO
from pathlib import Path

# 忽略警告
warnings.simplefilter("ignore", FutureWarning)

# ==================== 路徑設定 ====================
# 直接使用腳本所在目錄
BASE_DIR = Path(__file__).resolve().parent
# 固定輸出檔名
HTML_FILENAME = "HSI_CBBC_Ladder.html"
OUTPUT_PATH = BASE_DIR / HTML_FILENAME

strike_percent = ['10%', '9%', '8%', '7%', '6%', '5%', '4%', '3%', '2%', '1%', 'Spot',
                  '-1%', '-2%', '-3%', '-4%', '-5%', '-6%', '-7%', '-8%', '-9%', '-10%']

MAX_NETWORK_RETRIES = 5


# --- 內建 USDHKD 獲取函數 ---
def get_usdhkd_rate():
    """獲取實時 USD/HKD 匯率，失敗則返回 None"""
    try:
        ticker = yf.Ticker("HKD=X")
        data = ticker.history(period="1d")
        if not data.empty:
            rate = data['Close'].iloc[-1]
            ts = data.index[-1].strftime('%Y-%m-%d %H:%M')
            return rate, ts
    except Exception as e:
        print(f"匯率獲取失敗: {e}")
    return None, None


def download_and_parse_v5():
    url = "https://www.hkex.com.hk/eng/cbbc/search/cbbcfulllist.csv"
    headers = {"User-Agent": "Mozilla/5.0"}

    print("1. [下載] 正在下載 HKEX CBBC 全表...")
    try:
        r = None
        for attempt in range(1, MAX_NETWORK_RETRIES + 1):
            try:
                r = requests.get(url, headers=headers, timeout=30)
                r.raise_for_status()
                break
            except requests.RequestException as err:
                if attempt == MAX_NETWORK_RETRIES:
                    raise
                time.sleep(min(5, 2 ** attempt))

        try:
            text = r.content.decode('utf-16')
        except:
            text = r.content.decode('utf-8', errors='ignore')

        lines = text.split('\n')
        start_row = -1
        for i, line in enumerate(lines[:50]):
            if "Code" in line and "Bull" in line:
                start_row = i
                break

        if start_row == -1:
            raise Exception("找不到標題列！")

        raw_lines = lines[start_row:]
        clean_lines = [line for line in raw_lines if line.strip()]
        clean_text = '\n'.join(clean_lines)

        df = pd.read_csv(
            StringIO(clean_text),
            sep=None,
            engine="python",
            on_bad_lines='warn'
        )
        df.columns = df.columns.str.strip()

        # --- 欄位映射邏輯 ---
        rename_map = {}
        if 'Issuer' in df.columns:
            rename_map['Issuer'] = 'UL'
        if 'UL' in df.columns:
            rename_map['UL'] = 'Bull/Bear'

        if rename_map:
            df = df.rename(columns=rename_map)

        df = df.loc[:, ~df.columns.duplicated()]

        # 數值處理
        for col in ['Strike Level', 'Total Issue Size', 'O/S (%)', 'Entitlement Ratio^']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '', regex=True), errors='coerce')

        df['UL'] = df['UL'].astype(str).str.strip()
        df['Bull/Bear'] = df['Bull/Bear'].astype(str).str.strip()

        return df

    except Exception as e:
        print(f"[解析錯誤] {e}")
        raise


def get_hsi_cbbc(df):
    print("2. [篩選] 正在篩選 HSI...")
    mask = (
            df['UL'].str.upper().str.contains('HSI') |
            df['UL'].str.upper().str.contains('HANG SENG') |
            df['UL'].str.contains('恒生')
    )
    return df[mask].copy()


def get_hsi_price():
    print("3. [市價] 獲取 HSI 價格...")
    for attempt in range(1, MAX_NETWORK_RETRIES + 1):
        try:
            hist = yf.Ticker("^HSI").history(period="5d")
            close = hist['Close'].iloc[-1]
            date = hist.index[-1].strftime('%Y-%m-%d')
            print(f"   -> {close:,.2f} ({date})")
            return round(float(close), 2), date
        except Exception:
            time.sleep(1)
    return 19200.0, dt.datetime.now().strftime('%Y-%m-%d')


def build_price_detail(df_hsi, spot):
    columns = ['Price', 'Total_KO_$', 'Bull_KO_$', 'Bear_KO_$', 'Note']
    if df_hsi is None or df_hsi.empty:
        return pd.DataFrame(columns=columns)

    df = df_hsi.copy()
    if 'notional' not in df.columns:
        df['notional'] = (
                df['Entitlement Ratio^'] * df['Total Issue Size'] / 100 * spot)

    df['notional'] = df['notional'].fillna(0)
    df = df.dropna(subset=['Strike Level'])
    if df.empty:
        return pd.DataFrame(columns=columns)

    summary = (
        df.groupby(['Strike Level', 'Bull/Bear'])['notional']
        .sum()
        .unstack(fill_value=0)
    )

    for direction in ['Bull', 'Bear']:
        if direction not in summary.columns:
            summary[direction] = 0

    summary = summary[['Bull', 'Bear']]
    summary = summary.rename(columns={'Bull': 'Bull_KO_$', 'Bear': 'Bear_KO_$'})
    summary['Total_KO_$'] = summary['Bull_KO_$'] + summary['Bear_KO_$']

    price_detail = summary.reset_index().rename(columns={'Strike Level': 'Price'})
    price_detail = price_detail.sort_values('Price', ascending=False).reset_index(drop=True)

    # 將金額列轉換為百萬單位
    for col in ['Total_KO_$', 'Bull_KO_$', 'Bear_KO_$']:
        price_detail[col] = price_detail[col] / 1e6
        price_detail[col] = price_detail[col].round(0)

    price_detail['Note'] = ''
    if not price_detail.empty:
        closest_idx = (price_detail['Price'] - spot).abs().idxmin()
        price_detail.loc[closest_idx, 'Note'] = 'Spot'

    return price_detail[['Price', 'Total_KO_$', 'Bull_KO_$', 'Bear_KO_$', 'Note']]


def calculate_ladder(df_hsi, spot):
    if df_hsi.empty: return None
    print("4. [計算] 生成 Ladder...")

    df_hsi['notional'] = (df_hsi['Entitlement Ratio^'] * df_hsi['Total Issue Size'] / 100 * spot).fillna(0)

    bull = df_hsi[df_hsi['Bull/Bear'] == 'Bull']
    bear = df_hsi[df_hsi['Bull/Bear'] == 'Bear']

    ladder = pd.DataFrame(index=strike_percent)
    ladder['Strike'] = [spot if x == 'Spot' else round(spot * (1 + int(x.strip('%')) / 100), 2) for x in ladder.index]
    ladder['Bear (M)'] = 0
    ladder['Bull (M)'] = 0

    for i in range(1, 11):
        lower, upper = spot * (1 + (i - 1) / 100), spot * (1 + i / 100)
        m = (bear['Strike Level'] > lower) & (bear['Strike Level'] <= upper)
        ladder.loc[f'{i}%', 'Bear (M)'] = int(bear.loc[m, 'notional'].sum() / 1e6)

        upper_b, lower_b = spot * (1 - (i - 1) / 100), spot * (1 - i / 100)
        m_b = (bull['Strike Level'] < upper_b) & (bull['Strike Level'] >= lower_b)
        ladder.loc[f'-{i}%', 'Bull (M)'] = int(bull.loc[m_b, 'notional'].sum() / 1e6)

    bear_idx = [f'{i}%' for i in range(1, 11) if f'{i}%' in ladder.index]
    if bear_idx: ladder.loc[bear_idx, 'Bear Accu'] = ladder.loc[bear_idx, 'Bear (M)'].cumsum()

    bull_idx = [f'-{i}%' for i in range(1, 11) if f'-{i}%' in ladder.index]
    if bull_idx: ladder.loc[bull_idx, 'Bull Accu'] = ladder.loc[bull_idx, 'Bull (M)'].cumsum()

    ladder['CBBC KO'] = ladder['Bear (M)'] + ladder['Bull (M)']
    ladder.loc['Spot', ['Bear (M)', 'Bull (M)']] = ['SPOT', spot]
    ladder.loc['Spot', 'CBBC KO'] = 0

    cols = ['Strike', 'CBBC KO', 'Bear (M)', 'Bull (M)', 'Bear Accu', 'Bull Accu']
    ladder = ladder[[c for c in cols if c in ladder.columns]]

    return ladder.fillna(0)


def format_value_for_html(value, fmt):
    try:
        if pd.isna(value): return ''
        if isinstance(value, str): return value
        return format(float(value), fmt)
    except:
        return value


def apply_html_number_format(df, format_map):
    for col, fmt in format_map.items():
        if col in df.columns:
            df[col] = df[col].apply(lambda v: format_value_for_html(v, fmt))
    return df


def export_html(ladder, price_detail, spot, m_date, output_path, fx_rate):
    sheets = []
    if ladder is not None: sheets.append(('Ladder', ladder, True))
    if price_detail is not None and not price_detail.empty:
        sheets.append(('price_detail', price_detail, False))

    if not sheets: return

    default_tab = sheets[0][0]
    tab_buttons = []
    tab_contents = []

    columns_to_hide = ['Bear (M)', 'Bull (M)', 'Bear Accu', 'Bull Accu']

    # 隱藏 Breakdown 欄位
    price_detail_hide = [
        'Bull/Bear',
        'Note',
        'Bull_KO_$ (mio)',
        'Bear_KO_$ (mio)'
    ]

    for name, df, show_index in sheets:
        tab_id = f"tab-{name}"
        safe_name = name.lower().replace(' ', '_')
        table_id = f"table-{safe_name}"
        button = f'<button class="tab-link" data-tab="{name}">{name}</button>'
        tab_buttons.append(button)

        df_render = df.copy()
        render_idx = show_index

        if name == 'Ladder':
            df_render = df_render.drop(columns=[c for c in columns_to_hide if c in df_render.columns])
            df_render = df_render.reset_index().rename(columns={'index': 'Range'})
            render_idx = False
            # [修改] 這裡的 Key 也必須對應重命名後的欄位
            df_render = apply_html_number_format(df_render, {'Strike': ',.2f', 'Knock-out notional($mio)': ',.0f'})

        elif name == 'price_detail':
            df_render = df_render.drop(columns=[c for c in price_detail_hide if c in df_render.columns])
            df_render.columns.name = None
            render_idx = False

            # [修改] 這裡的 Key 也必須對應重命名後的欄位
            fmt_map = {
                'Price': ',.0f',
                'Knock-out notional($mio)': ',.0f'
            }
            df_render = apply_html_number_format(df_render, fmt_map)

        table_html = df_render.to_html(classes='dataframe', index=render_idx, border=0, justify='center',
                                       table_id=table_id)
        tab_contents.append(f'<div id="{tab_id}" class="tab-content">{table_html}</div>')

    html_template = f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8" />
<style>
    body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f7f7f7; }}
    .meta {{ margin-bottom: 20px; color: #555; }}
    .tab-link {{ border: none; padding: 10px 20px; margin-right: 10px; cursor: pointer; background-color: #e0e0e0; border-radius: 4px; }}
    .tab-link.active {{ background-color: #007bff; color: #fff; }}
    .tab-content {{ display: none; background-color: #fff; padding: 20px; border-radius: 6px; }}
    table.dataframe {{ border-collapse: collapse; width: 100%; font-size: 12px; }}
    table.dataframe th, table.dataframe td {{ border: 1px solid #ccc; padding: 6px; text-align: center; }}
    table.dataframe thead {{ background-color: #f0f0f0; }}
</style>
</head>
<body>
    <h3>恆指牛熊重貨區 HSI CBBC Ladder</h3>
    <div class="meta">Date: {m_date} | Spot: {spot:.2f} {f"| USDHKD: {fx_rate:.4f}" if fx_rate else ""}</div>
    <div class="tabs">{"".join(tab_buttons)}</div>
    {"".join(tab_contents)}
<script>
    function showTab(name) {{
        document.querySelectorAll('.tab-content').forEach(c => c.style.display = 'none');
        document.querySelectorAll('.tab-link').forEach(b => b.classList.remove('active'));
        document.getElementById('tab-' + name).style.display = 'block';
        document.querySelector(`button[data-tab="${{name}}"]`).classList.add('active');
    }}
    document.querySelectorAll('.tab-link').forEach(b => b.addEventListener('click', function() {{ showTab(this.dataset.tab); }}));

    function colorize(id, colName) {{
        const table = document.getElementById(id);
        if(!table) return;
        let colIdx = -1;
        table.querySelectorAll('th').forEach((th, i) => {{ if(th.textContent.trim() === colName) colIdx = i; }});
        if(colIdx === -1) return;

        let maxVal = 0;
        const rows = table.querySelectorAll('tbody tr');
        rows.forEach(row => {{
            const val = parseFloat(row.cells[colIdx].textContent.replace(/,/g, ''));
            if(!isNaN(val) && val > maxVal) maxVal = val;
        }});

        rows.forEach(row => {{
            const cell = row.cells[colIdx];
            const val = parseFloat(cell.textContent.replace(/,/g, ''));
            if(!isNaN(val) && val > 0) {{
                const ratio = Math.min(val / maxVal, 1);
                cell.style.backgroundColor = `rgba(0, 255, 0, ${{ratio * 0.6}})`;
                if(val > 1000) {{ cell.style.color = 'red'; cell.style.fontWeight = 'bold'; }}
            }}
        }});
    }}

    showTab('{default_tab}');
    // [修改] 對應新的欄位名稱進行染色
    colorize('table-ladder', 'Knock-out notional($mio)');
    colorize('table-price_detail', 'Knock-out notional($mio)');
</script>
</body>
</html>"""

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_template)
    print(f"[成功] HTML 已生成: {output_path}")


def main():
    print("\n=== HSI CBBC Tool (V10 Final Rename) ===\n")
    try:
        df_all = download_and_parse_v5()
        df_hsi = get_hsi_cbbc(df_all)
        spot, m_date = get_hsi_price()
        rate, fx_ts = get_usdhkd_rate()

        if rate:
            print(f"   -> 1 USD = {rate:.4f} HKD")
        else:
            print("   -> 匯率獲取失敗，維持 HKD 顯示")

        ladder = calculate_ladder(df_hsi, spot)
        price_detail = build_price_detail(df_hsi, spot)

        # 貨幣轉換
        if rate:
            if ladder is not None and 'CBBC KO' in ladder.columns:
                ladder['CBBC KO'] = ladder['CBBC KO'] / rate
            if price_detail is not None:
                for col in ['Total_KO_$', 'Bull_KO_$', 'Bear_KO_$']:
                    if col in price_detail.columns:
                        price_detail[col] = price_detail[col] / rate

        # [修改] 欄位重新命名 (統一為 "Knock-out notional($mio)")
        if ladder is not None:
            ladder = ladder.rename(columns={'CBBC KO': 'Knock-out notional($mio)'})
        if price_detail is not None:
            price_detail = price_detail.rename(columns={
                'Total_KO_$': 'Knock-out notional($mio)',  # 這裡改名
                'Bull_KO_$': 'Bull_KO_$ (mio)',
                'Bear_KO_$': 'Bear_KO_$ (mio)'
            })

        export_html(ladder, price_detail, spot, m_date, str(OUTPUT_PATH), rate)

    except Exception as e:
        print(f"[嚴重錯誤] {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()