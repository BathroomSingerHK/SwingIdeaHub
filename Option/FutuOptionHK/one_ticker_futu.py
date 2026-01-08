import time
import pandas as pd
from datetime import datetime
from futu import *


# ===========================
# 配置區域
# ===========================
class Config:
    HOST = '127.0.0.1'
    PORT = 11111

    # 目標參數
    STOCK_CODE = 'HK.00700'  # 正股: 騰訊
    TARGET_DATE = '2026-01-29'  # 到期日 (260129)
    TARGET_STRIKE = 630.0  # 行權價

    # 這裡只作為本地篩選用，不傳入 API
    WANTED_TYPE = 'CALL'  # 'CALL' or 'PUT'

    # 獲取成交紀錄筆數
    MAX_NUM = 1000


class SingleOptionAnalyzer:
    def __init__(self):
        self.ctx = OpenQuoteContext(host=Config.HOST, port=Config.PORT)

    def close(self):
        self.ctx.close()

    def find_specific_option(self):
        """
        步驟 1: 根據日期和行權價，自動查找期權代碼
        """
        print(
            f">> 正在搜尋 {Config.STOCK_CODE} 於 {Config.TARGET_DATE} 到期, 行權價 {Config.TARGET_STRIKE} 的 {Config.WANTED_TYPE}...")

        # [修正點]：移除 index_option_type 參數
        # 個股期權不需要該參數，我們抓下來後自己篩選
        ret, chain = self.ctx.get_option_chain(
            code=Config.STOCK_CODE,
            start=Config.TARGET_DATE,
            end=Config.TARGET_DATE
        )

        if ret != RET_OK:
            print(f"  [Error] 無法獲取期權鏈: {chain}")
            return None

        if chain.empty:
            print(f"  [Error] 找不到 {Config.TARGET_DATE} 到期的期權合約。")
            return None

        # [新增]：本地篩選 Call 或 Put
        # option_type 欄位通常是 "CALL" 或 "PUT"
        chain = chain[chain['option_type'] == Config.WANTED_TYPE]

        if chain.empty:
            print(f"  [Error] 該日期沒有 {Config.WANTED_TYPE} 期權。")
            return None

        # 篩選行權價 (精確匹配)
        # 為了防止浮點數誤差，使用微小範圍比較
        target_opt = chain[abs(chain['strike_price'] - Config.TARGET_STRIKE) < 0.001]

        if target_opt.empty:
            print(f"  [Error] 找到期權，但沒有行權價為 {Config.TARGET_STRIKE} 的合約。")
            # 列出附近幾個行權價供參考
            print("  附近的行權價有:", chain['strike_price'].sort_values().unique().tolist()[:10])
            return None

        # 成功找到
        option_code = target_opt.iloc[0]['code']
        option_name = target_opt.iloc[0]['name']
        print(f"  -> 成功鎖定合約: {option_name} ({option_code})")
        return option_code

    def get_transaction_record(self, option_code):
        """
        步驟 2: 獲取逐筆成交紀錄 (Ticker) 並分析買賣方向
        """
        print(f"\n>> 正在下載 {option_code} 的逐筆成交紀錄 (Ticker)...")

        ret, ticker_data = self.ctx.get_rt_ticker(option_code, num=Config.MAX_NUM)

        if ret != RET_OK:
            print(f"  [Error] 獲取 Ticker 失敗: {ticker_data}")
            return

        if ticker_data.empty:
            print("  [Info] 該合約近期沒有成交紀錄。")
            return

        print(f"  -> 下載成功，共 {len(ticker_data)} 筆交易。")

        # 統計分析
        buy_df = ticker_data[ticker_data['ticker_direction'] == 'BUY']
        sell_df = ticker_data[ticker_data['ticker_direction'] == 'SELL']
        neutral_df = ticker_data[ticker_data['ticker_direction'] == 'NEUTRAL']

        buy_vol = buy_df['volume'].sum()
        sell_vol = sell_df['volume'].sum()
        total_vol = ticker_data['volume'].sum()

        buy_amt = buy_df['turnover'].sum()
        sell_amt = sell_df['turnover'].sum()

        print("\n" + "=" * 50)
        print(f"【交易方向分析報告】 {option_code}")
        print("=" * 50)
        print(f"總成交量    : {total_vol} 股")
        print(f"總成交筆數  : {len(ticker_data)} 筆")
        print("-" * 30)
        print(f"🔴 主動買入 (Long/Buy)  : {buy_vol} 股 ({len(buy_df)} 筆) -> 資金: ${buy_amt:,.0f}")
        print(f"🟢 主動賣出 (Short/Sell): {sell_vol} 股 ({len(sell_df)} 筆) -> 資金: ${sell_amt:,.0f}")
        print(f"⚪ 中性盤   (Neutral)   : {neutral_df['volume'].sum()} 股")
        print("-" * 30)

        if buy_vol > sell_vol:
            print("📈 結論: 買盤力道較強 (Net Buy)")
        elif sell_vol > buy_vol:
            print("📉 結論: 賣盤力道較強 (Net Sell)")
        else:
            print("⚖️ 結論: 買賣平衡")

        print("\n【最近 20 筆成交明細】")
        display_cols = ['time', 'price', 'volume', 'ticker_direction', 'turnover']
        print(ticker_data[display_cols].tail(20).to_string(index=False))

        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        filename = f"ticker_{option_code}_{timestamp}.xlsx"
        ticker_data.to_excel(filename, index=False)
        print(f"\n[成功] 完整紀錄已保存至: {filename}")

    def run(self):
        try:
            target_code = self.find_specific_option()
            if target_code:
                self.get_transaction_record(target_code)
        except Exception as e:
            print(f"發生錯誤: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    analyzer = SingleOptionAnalyzer()
    try:
        analyzer.run()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        analyzer.close()