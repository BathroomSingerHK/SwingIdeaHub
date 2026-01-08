import time
import pandas as pd
from datetime import datetime, timedelta
from futu import *

# ===========================
# 0. 用戶提供的美股 Ticker 列表 (原始字符串)
# ===========================
RAW_US_TICKER_STR = """
CCCX
OMC
TTD
DV
MGNI
RKLB
KTOS
CRS
AIRO
KRMN
GE
BA
AVAV
HWM
NOC
RTX
BWXT
ATI
AIR
LHX
LMT
MRCY
GD
AXON
HII
DRS
LUNR
RDW
FLY
BYRN
TXT
EVTL
VOYG
ACHR
DE
CNH
ADM
BG
DAR
FWRD
CHRW
UPS
FDX
GXO
ASTS
LBTYA
CCOI
GSAT
IRDM
LUMN
AA
CENX
REAL
SFIX
TJX
BURL
TDUP
ROST
AEO
GAP
VSCO
ANF
URBN
RL
LEVI
VFC
AS
TPR
FIGS
LULU
CPRI
PVH
CRI
UAA
APP
APPS
FIG
IREN
CLSK
CIFR
ARQQ
PGY
QBTS
FICO
PLTR
WULF
IDCC
CWAN
ROP
PAR
CRCL
TEAM
SNPS
ZETA
SOUN
RIOT
YOU
ADBE
DBX
BOX
BTBT
BILL
FRSH
PEGA
VERX
BTDR
INTA
MARA
CLBT
ASAN
TTAN
NCNO
DDOG
INTU
ZM
AMPL
BLND
DT
TYL
PD
WDAY
CDNS
QTWO
AUR
PCOR
SEMR
CRM
JAMF
CFLT
GWRE
AI
KVYO
HUBS
ADSK
AVPT
U
DOCU
CRNC
IOT
PRCH
NN
FIVN
SPT
ESTC
RNG
RZLV
CORZ
BRZE
ABTC
NTNX
BMNR
SAIL
BSY
MSTR
GLXY
GLD
BEN
BK
CNS
NTRS
TROW
OBDC
STT
IVZ
NMFC
PFLT
MSDL
ARES
BLK
BXSL
MAIN
FSK
BX
DBRG
ARCC
OCSL
KKR
HTGC
TRIN
WT
OWL
CG
CSWC
ASST
STLA
RACE
GM
THO
RIVN
TSLA
F
LCID
SLDP
KDK
MBLY
QS
MOD
APTV
BWA
DAN
AXL
CVNA
ORLY
AZO
AAP
EVGO
LAD
CWH
MNRO
KMX
QURE
MLTX
TSHA
SLNO
CDTX
PGEN
ARQT
GOSS
APLS
AUPH
CELC
GILD
INSM
PRAX
RIGL
PTGX
STOK
VRTX
GRAL
AVXL
ALNY
RNA
PCVX
NVAX
CTMX
DAWN
KRYS
ABBV
UTHR
GERN
SMMT
CRSP
SPRY
AMGN
CPRX
SNDX
EXAS
MRUS
FOLD
DVAX
RVMD
AGIO
INCY
IONS
ALKS
MNKD
ACAD
JANX
REGN
VERA
NTLA
ABSI
RZLT
ADMA
ARDX
SRPT
TGTX
BBIO
MRNA
SRRK
CYTK
NAMS
ROIV
MLYS
EXEL
SANA
BCRX
NTRA
ZYME
IBRX
BIIB
NRIX
URGN
ANAB
IOVA
MDGL
HALO
RARE
BMRN
NBIX
ORIC
TWST
VKTX
TVTX
ARWR
ARCT
IMVT
KOD
COGT
BEAM
RCUS
OCGN
PRME
CAI
KURA
IMNM
BHVN
RXRX
OLMA
TAP
FOXA
PSKY
ETSY
MELI
EBAY
GRPN
AMZN
CPNG
M
OLLI
GLBE
KSS
BLDR
MAS
TT
AAON
CARR
JCI
AOS
WMS
OC
REZI
SIRI
CMCSA
SATS
CHTR
XPO
ODFL
KNX
JBHT
ARCB
WERN
RXO
SRAD
LVS
WYNN
GENI
BYD
MGM
SGHC
PENN
BRSL
FLUT
RSI
DKNG
CZR
SBET
CNR
LEU
UEC
BTU
LTBR
BETR
LDI
PFSI
RKT
UWMC
LYB
WLK
PCT
DOW
TROX
OLN
ONDS
FFIV
CSCO
EXTR
NTGR
VIAV
CIEN
COMM
LITE
HLIT
ANET
ADTN
VSAT
AAOI
BBY
GME
AGX
AMRC
CTRI
WSC
EME
STRL
PWR
PRIM
MTZ
TPC
FLR
CMI
CAT
PCAR
MVST
VMC
CRH
GRMN
SONO
SOFI
LC
FIGR
EZPW
DAVE
ALLY
COF
SLM
SYF
OMF
AXP
UPST
TREE
GDOT
WMT
COST
TGT
BJ
DG
DLTR
FCX
SCCO
VRRM
CNXC
STZ
GCT
GPC
POOL
LKQ
WFC
CMA
BAC
FITB
C
USB
JPM
KEY
PNC
NU
INTR
HUN
CC
CRBG
APO
TMC
UAMY
USAR
NB
IE
ABAT
MP
IDR
CRML
VSTS
CTAS
CPRT
ACVA
DUOL
LRN
COUR
UTI
UDMY
EXC
AEP
NEE
PNW
XEL
PCG
SO
DUK
PPL
HE
EVRG
ES
FE
ALE
EIX
NRG
ETR
CEG
OKLO
SES
GNRC
RUN
POWL
HUBB
NXT
AYI
ROK
EOSE
ATKR
NVT
SHLS
ARRY
ETN
VRT
EMR
ENVX
AMPX
PLUG
AMSC
FLNC
APH
GLW
COHR
AEVA
RCAT
OUST
MIR
EVLV
TRMB
CGNX
LASR
JBL
FLEX
TTMI
SANM
TTEK
WM
RSG
NVRI
CTVA
SMG
MOS
CF
FMC
ICE
CBOE
MIAX
MCO
TW
FDS
SPGI
CME
NDAQ
MKTX
COIN
GEMI
BKKT
BLSH
CHEF
UNFI
ANDE
PFGC
SYY
KR
CART
ACI
SFM
GO
WWW
SHOO
ONON
CROX
BIRK
NKE
DECK
UGI
AU
NEM
CDE
RGLD
MUX
MCK
COR
CAH
TMDX
SYK
INSP
HOLX
PODD
BDX
EW
IDXX
MDT
HSDT
DXCM
ABT
CLPT
BSX
BAX
ATEC
INMD
PRCT
ISRG
BFLY
NVCR
GEHC
GKOS
TNDM
QDEL
ACHC
THC
HCA
BKD
PACS
HIMS
DVA
DGX
CI
WGS
CVS
GH
RDNT
NEO
OPCH
HNGE
BTSG
AMN
ESTA
HAE
NEOG
ALGN
COO
LNTH
SOLV
VEEV
PHR
WAY
DOCS
TDOC
CERT
GDRX
SDGR
NNE
GEV
PSIX
BE
SMR
HD
LOW
FND
LEN
PHM
KBH
GRBK
TOL
DHI
W
RH
WSM
BBBY
VIK
ABNB
HLT
RCL
NCLH
MMYT
CCL
MAR
BKNG
EXPE
HELE
SN
WHR
CL
CLX
KMB
PG
CHD
REYN
NWL
PAYX
PAYC
ADP
ALIT
UPWK
FVRR
AES
HNRG
TLN
VST
HON
MMM
LIN
APD
RR
SYM
WOR
GTES
OTIS
FTV
FLS
MLI
ITW
DOV
ITT
XYL
SWK
JBTM
HI
MMC
RYAN
AON
AJG
ARX
CVX
XOM
OXY
T
VZ
IDT
UNIT
TTWO
EA
RBLX
RDDT
META
EVER
FUBO
BMBL
GTM
GOOGL
PINS
TRIP
IAC
GRND
MTCH
DJT
SNAP
YELP
RUM
ANGI
VRSN
DOCN
WYFI
MDB
GDDY
APLD
NET
TWLO
AKAM
OKTA
SNOW
CRWV
FSLY
WIX
BGC
LPLA
HOOD
SCHW
VIRT
IBKR
EVR
GS
RJF
MS
ETOR
MRX
JEF
XP
BULL
TSSI
IBM
ACN
CTSH
EPAM
GLOB
DXC
KD
BBAI
IT
GDYN
PLNT
PLAY
PRKS
MTN
FUN
HAS
MAT
PII
TRON
BC
MODG
YETI
PTON
OSCR
MET
PRU
AFL
LNC
UNM
GNW
BHF
TEM
DNA
QGEN
ADPT
TMO
NAGE
DHR
ABCL
IQV
A
CRL
RGEN
WAT
ILMN
AVTR
TECH
BRKR
TXG
CLOV
ELV
CNC
UNH
MOH
HQY
HUM
ALHC
SBLK
ZIM
BALL
HOG
NFLX
SPOT
TKO
DIS
LYV
IMAX
AMC
CNK
WBD
ROKU
LION
MSGS
SPHR
ANGX
SRE
BKH
CMS
ED
WEC
CNP
AEE
D
NI
PEG
PBI
PTEN
RIG
VAL
HP
NE
NBR
BORR
KGS
AROC
NOV
TDW
FTI
WFRD
OII
HAL
SLB
BKR
LBRT
AESI
TTI
SEI
SOC
CTRA
CRK
RRC
EXE
CNX
COP
DVN
EOG
AR
EQT
FANG
APA
PR
CHRD
SM
CRGY
CIVI
OVV
MTDR
TALO
TPL
MUR
KOS
NOG
VG
CLMT
VLO
GPRE
DINO
PSX
CVI
GEVO
MPC
DK
PBF
TRGP
KNTK
NEXT
LNG
WMB
AM
KMI
HESM
OKE
DHT
NAT
GLNG
NFE
LPG
STNG
INSW
FRO
CHWY
SIG
EYE
ULTA
TSCO
ASO
DKS
WRBY
BBWI
BBW
FIVE
SBH
UPBD
VITL
SMPL
GIS
MDLZ
SJM
CAG
KHC
K
TSN
CALM
PPC
LW
FLO
HSY
CPB
NOMD
FRPT
DOLE
HRL
THS
JBS
MKC
ORBS
SW
IP
SON
GPK
AMCR
SEE
ALK
UAL
LUV
SKYW
AAL
DAL
JBLU
JOBY
ULCC
UP
CAR
HTZ
GRAB
UBER
LYFT
EL
KVUE
BRBR
ELF
EPC
HLF
COTY
ODD
TLRY
AMLX
LQDA
OCUL
SNDL
CRNX
XERS
LENZ
HRMY
ATAI
CORT
JNJ
VTRS
AQST
RPRX
LLY
JAZZ
AVDL
WVE
SUPN
PFE
HROW
ESPR
ELAN
BMY
AXSM
INVA
MRK
PRGO
ZTS
AMRX
MNMD
OGN
EYPT
MBX
NUVB
NKTR
ARVN
TRVI
CRMD
ZVRA
RAPP
PHAT
ACGL
ALL
PGR
AIG
CB
HIG
TRV
SLDE
ORI
ROOT
LMND
NWSA
CSX
NSC
UNP
HHH
LB
OPEN
EXPI
CBRE
CSGP
ZG
NMRK
COMP
HOUS
OZK
HBAN
EBC
MTB
FLG
ZION
TFC
FHN
RF
VLY
TFIN
BANC
SNV
CFG
WAL
WBS
TBBK
ORA
XIFR
BKSY
PL
WLDN
TIC
BAH
EFX
INOD
J
TRU
LDOS
PSN
LZ
AMTM
BLMN
FWRG
EAT
WING
MCD
TXRH
CAKE
DRI
SBUX
YUM
YUMC
ARMK
CMG
CBRL
DASH
SG
WEN
DPZ
SHAK
BROS
CAVA
PZZA
SERV
DNUT
CXW
GEO
ENPH
AMKR
ACMR
SEDG
CAMT
ACLS
KLAC
TER
AMAT
LRCX
UCTT
ICHR
ONTO
PLAB
MKSI
ENTG
FORM
AEHR
POET
SKYT
RGTI
SMTC
AMD
BZAI
INTC
INDI
AVGO
NVDA
FSLR
QCOM
MRVL
MPWR
MCHP
ALGM
TXN
MU
MTSI
GFS
QRVO
SWKS
ADI
ALAB
RMBS
NVTS
ON
AMBA
OLED
CSIQ
KOPN
SYNA
NXPI
MXL
CRDO
PENG
LAES
HL
CELH
PRMB
MNST
KO
PEP
CCEP
KDP
COCO
STUB
SCI
ADT
HASI
SHW
ECL
IFF
ASPI
RPM
ALB
PPG
EMN
DD
PRM
CE
ASPN
METC
CLF
STLD
NUE
HCC
AMR
PATH
PRGS
CHKP
NTSK
PANW
CYBR
MSFT
NOW
GEN
FTNT
APPN
FROG
GTLB
CRWD
ORCL
RBRK
S
DLB
ZS
RPD
AIP
ADEA
MNDY
OS
CVLT
TDC
NBIS
DELL
SMCI
HPE
IONQ
AAPL
SNDK
HPQ
QUBT
NTAP
PSTG
WDC
STX
GT
MO
PM
TPB
XMTR
URI
GWW
AL
FTAI
FAST
AER
QXO
SITE
FERG
CNM
DXPE
PYPL
FIS
XYZ
WU
GPN
AFRM
V
RELY
TOST
MA
DLO
MQ
FOUR
STNE
CHYM
PAYO
PAY
KLAR
PAGS
SEZL
WBI
AWK
WTRG
TMUS
TDS
TIGO
GOGO
QQQ
"""


# 自動轉換函數：移除空白，添加 'US.' 前綴
def parse_us_tickers(raw_str):
    # 用換行符分割，去除首尾空白，過濾空行
    return [f"US.{t.strip()}" for t in raw_str.strip().split('\n') if t.strip()]


# ===========================
# 1. 基礎配置
# ===========================
class Config:
    HOST = '127.0.0.1'
    PORT = 11111

    # 使用解析函數生成目標股票列表
    TARGET_STOCKS = parse_us_tickers(RAW_US_TICKER_STR)

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

        # 美股快照頻率限制較嚴格，建議保持200或更低
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
            # 注意：美股某些熱門股票期權鏈非常長，如果請求超時，可考慮縮短日期範圍
            ret, chain = self.ctx.get_option_chain(
                code=stock,
                start=s_str,
                end=e_str,
                data_filter=None
            )

            if ret == RET_OK and not chain.empty:
                all_chains.append(chain)

            # 美股期權鏈數據量大，建議這裡增加一點延遲
            current_start = current_end + timedelta(days=1)
            time.sleep(3.0)

        if all_chains:
            return pd.concat(all_chains, ignore_index=True).drop_duplicates(subset=['code'])
        return pd.DataFrame()

    def run(self):
        print(f"啟動數據提取器 (Extraction Mode - US Market)...")
        print(f"目標股票數量: {len(Config.TARGET_STOCKS)}")
        all_data_frames = []

        for index, stock in enumerate(Config.TARGET_STOCKS):
            print(f"\n[{index + 1}/{len(Config.TARGET_STOCKS)}] 正在處理 {stock} ...")

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
            filename = f"us_option_raw_data_{timestamp}.xlsx"

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