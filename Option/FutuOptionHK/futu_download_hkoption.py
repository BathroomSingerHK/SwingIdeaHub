import time
import pandas as pd
from datetime import datetime, timedelta
from futu import *

# ===========================
# 1. 基礎配置
# ===========================
class Config:
    HOST = '127.0.0.1'
    PORT = 11111

    # 目標股票
    TARGET_STOCKS = [
        'HK.800000','HK.01810', 'HK.00700', 'HK.02318', 'HK.00939', 'HK.00388',
        'HK.01398', 'HK.09988', 'HK.00883', 'HK.02628', 'HK.03988',
        'HK.00941', 'HK.00005', 'HK.02333', 'HK.03690', 'HK.06030',
        'HK.01211', 'HK.00386', 'HK.01299', 'HK.00728', 'HK.01928',
        'HK.01088', 'HK.00981', 'HK.00175', 'HK.00762', 'HK.02601',
        'HK.09618', 'HK.00027', 'HK.03888', 'HK.00914', 'HK.02382',
        'HK.02018', 'HK.00857', 'HK.00688', 'HK.02269', 'HK.02388',
        'HK.00992', 'HK.00001', 'HK.00016', 'HK.02020', 'HK.01093',
        'HK.09999', 'HK.00823'
    ]
    # 掃描未來多少天的期權
    TENOR_DAYS = 60


class FutuDataExtractor:
    def __init__(self):
        # 連接 OpenD
        self.ctx = OpenQuoteContext(host=Config.HOST, port=Config.PORT)

    def close(self):
        self.ctx.close()

    def get_market_snapshot_safe(self, codes):
        """
        獲取實時快照 (Snapshot)
        這是我們獲取 Volume, Turnover, OI, Greeks 的唯一來源
        """
        if not codes: return pd.DataFrame()

        BATCH_SIZE = 200
        all_snaps = []

        print(f"  -> 正在下載 {len(codes)} 個合約的實時快照...")

        for i in range(0, len(codes), BATCH_SIZE):
            batch = codes[i: i + BATCH_SIZE]
            ret, data = self.ctx.get_market_snapshot(batch)
            if ret == RET_OK:
                all_snaps.append(data)
            else:
                print(f"  [Error] Snapshot failed for batch {i}: {data}")
            # 稍微停頓防止頻率限制
            time.sleep(2.8)

        if not all_snaps: return pd.DataFrame()
        return pd.concat(all_snaps, ignore_index=True)

    def get_option_chain_split(self, stock, total_days):
        """
        獲取期權鏈 (Chain)
        """
        all_chains = []
        start_date = datetime.now()
        end_date = start_date + timedelta(days=total_days)
        current_start = start_date

        while current_start < end_date:
            current_end = min(current_start + timedelta(days=25), end_date)
            s_str = current_start.strftime("%Y-%m-%d")
            e_str = current_end.strftime("%Y-%m-%d")

            # 請求時不帶 filter，確保先拿到所有合約代碼
            ret, chain = self.ctx.get_option_chain(
                code=stock,
                start=s_str,
                end=e_str,
                data_filter=None
            )

            if ret == RET_OK and not chain.empty:
                all_chains.append(chain)

            current_start = current_end + timedelta(days=1)
            time.sleep(2.8) # 稍微縮短一點等待時間

        if all_chains:
            return pd.concat(all_chains, ignore_index=True).drop_duplicates(subset=['code'])
        return pd.DataFrame()

    def run(self):
        print(f"啟動數據提取器 (Extraction Mode)...")
        all_data_frames = []

        for stock in Config.TARGET_STOCKS:
            print(f"\n>> 正在處理 {stock} ...")

            # =======================================================
            # NEW STEP: 先獲取正股(Underlying)的當前價格
            # =======================================================
            ul_price = 0.0
            ret, stock_snap = self.ctx.get_market_snapshot([stock])
            if ret == RET_OK and not stock_snap.empty:
                ul_price = stock_snap['last_price'].iloc[0]
                print(f"  -> [Info] 正股 {stock} 當前價格: {ul_price}")
            else:
                print(f"  -> [Warning] 無法獲取正股 {stock} 價格，將設為 0")

            # 1. 獲取鏈 (只為了拿 Code, Strike, Expiry, Type)
            chain = self.get_option_chain_split(stock, Config.TENOR_DAYS)
            if chain.empty:
                print("  無合約數據 (Chain Empty)。")
                continue

            print(f"  -> 找到 {len(chain)} 張合約。")
            codes = chain['code'].tolist()

            # 2. 獲取快照 (這是真正的數據源)
            snap = self.get_market_snapshot_safe(codes)
            if snap.empty:
                print("  無法獲取快照數據。")
                continue

            # =======================================================
            # 3. 數據組裝 (Extraction)
            # =======================================================
            # 欄位：代碼, 名稱, 行權時間, 行權價, 類型, 正股
            identity_cols = ['code', 'name', 'strike_time', 'strike_price', 'option_type', 'stock_owner']
            identity_cols = [c for c in identity_cols if c in chain.columns]
            df_identity = chain[identity_cols].copy()

            # 我們只保留 Snapshot 裡面的「市場信息」
            # 注意：這裡移除了 'ulPrice'，因為我們使用上面獲取的 ul_price 變數
            market_cols = [
                'code',
                'last_price', 'volume', 'turnover', 'option_open_interest',
                'option_implied_volatility', 'option_delta', 'option_gamma', 'option_vega'
            ]

            # 確保 snapshot 有這些欄位
            for col in market_cols:
                if col not in snap.columns:
                    snap[col] = 0

            df_market = snap[market_cols].copy()

            # 4. 合併 (以 Code 為準)
            final_df = pd.merge(df_identity, df_market, on='code', how='inner')

            # =======================================================
            # NEW STEP: 填入正股價格 column
            # =======================================================
            final_df['ul_price'] = ul_price

            # 5. 重命名欄位 (讓 Excel 更好看)
            final_df.rename(columns={
                'option_open_interest': 'OpenInterest',
                'option_implied_volatility': 'IV',
                'option_delta': 'Delta',
                'option_gamma': 'Gamma',
                'option_vega': 'Vega',
                'strike_price': 'Strike',
                'strike_time': 'Expiry',
                'last_price': 'Price'
            }, inplace=True)

            all_data_frames.append(final_df)
            print(f"  -> {len(final_df)} 條數據已提取。")

        # 6. 保存結果
        if all_data_frames:
            full_df = pd.concat(all_data_frames, ignore_index=True)

            # 可選：重新排列欄位順序，把 ul_price 放在前面一點方便查看
            cols = full_df.columns.tolist()
            # 簡單優化欄位順序：把 ul_price 移到 Strike 附近
            if 'ul_price' in cols and 'Strike' in cols:
                cols.remove('ul_price')
                idx = cols.index('Strike')
                cols.insert(idx, 'ul_price')
                full_df = full_df[cols]

            print("\n" + "=" * 80)
            print("【Extraction Result Preview】")
            print("=" * 80)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 200)
            print(full_df.head(10).to_string(index=False))

            # 存檔
            timestamp = datetime.now().strftime('%Y%m%d_%H%M')
            filename = f"hk_option_raw_data_{timestamp}.xlsx"

            try:
                full_df.to_excel(filename, index=False)
                print(f"\n[成功] 所有原始數據已保存至: {filename}")
            except Exception as e:
                print(f"\n[Error] 保存 Excel 失敗: {e}")
                full_df.to_csv(filename.replace('.xlsx', '.csv'), index=False, encoding='utf-8-sig')
        else:
            print("\n沒有提取到任何數據。")


if __name__ == "__main__":
    extractor = FutuDataExtractor()
    try:
        extractor.run()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        extractor.close()