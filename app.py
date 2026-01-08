import streamlit as st
from streamlit_option_menu import option_menu
import streamlit.components.v1 as components
import os
import sys
import glob
import time

# ==========================================
# 1. Page Configuration
# ==========================================
st.set_page_config(
    page_title="Cross-Asset Quant Research",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================
# 2. Custom CSS
# ==========================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Roboto+Mono:wght@400;500;700&display=swap');

    .stApp { background: transparent !important; }

    html, body, [class*="css"] {
        font-family: 'Inter', 'Segoe UI', 'Microsoft JhengHei', sans-serif;
        color: #e2e8f0;
    }

    @media (min-width: 768.1px) {
        header { visibility: hidden !important; }
        [data-testid="stSidebarCollapseButton"] { display: none !important; }
        section[data-testid="stSidebar"] button { display: none !important; }
        [data-testid="stToolbar"], [data-testid="stHeaderActionElements"] { visibility: hidden !important; display: none !important; }
        #MainMenu { visibility: hidden !important; display: none !important; }
    }

    footer { visibility: hidden !important; display: none !important; }
    div[data-testid="stDecoration"] { display: none !important; }

    .fixed-bg {
        position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; 
        z-index: -1; 
        background-color: #020617;
        background-image: 
            linear-gradient(to right, rgba(255, 255, 255, 0.05) 1px, transparent 1px),
            linear-gradient(to bottom, rgba(255, 255, 255, 0.05) 1px, transparent 1px);
        background-size: 50px 50px;
        mask-image: linear-gradient(to bottom, black 40%, transparent 100%);
        -webkit-mask-image: linear-gradient(to bottom, black 40%, transparent 100%);
    }

    .fixed-blobs {
        position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; 
        z-index: -1;
        background: 
            radial-gradient(circle at 10% 10%, rgba(79, 70, 229, 0.15) 0%, transparent 40%),
            radial-gradient(circle at 90% 20%, rgba(14, 165, 233, 0.15) 0%, transparent 40%),
            radial-gradient(circle at 30% 90%, rgba(16, 185, 129, 0.1) 0%, transparent 40%);
        filter: blur(60px); pointer-events: none;
    }

    section[data-testid="stSidebar"] {
        background-color: #111827; 
        border-right: 1px solid #374151;
    }

    .profile-card {
        background: rgba(17, 24, 39, 0.7); backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.08); border-radius: 16px;
        padding: 25px; text-align: center;
    }

    .custom-footer {
        margin-top: 50px; padding-top: 20px; border-top: 1px solid rgba(255, 255, 255, 0.1);
        text-align: center; color: #94a3b8; font-size: 0.8rem;
    }
</style>

<div class="fixed-bg"></div>
<div class="fixed-blobs"></div>
""", unsafe_allow_html=True)


# ==========================================
# 3. Helper Functions
# ==========================================

def load_weekly_analysis():
    file_path = os.path.join("WeeklyContent", "latest_analysis.md")
    return open(file_path, 'r', encoding='utf-8').read() if os.path.exists(file_path) else "⚠️ Analysis not found."


def load_html_file(file_path):
    return open(file_path, 'r', encoding='utf-8').read() if os.path.exists(
        file_path) else f"⚠️ File not found: {file_path}"


def get_latest_file_content(folder_path, pattern="*.html"):
    if not os.path.exists(folder_path): return None, "Dir not found"
    list_of_files = glob.glob(os.path.join(folder_path, pattern))
    if not list_of_files: return None, "No files found"
    latest_file = max(list_of_files, key=os.path.getctime)
    with open(latest_file, 'r', encoding='utf-8') as f:
        return f.read(), os.path.basename(latest_file)


def load_stock_dna_with_injection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(current_dir, "FamaFrench", "index.html")
    if not os.path.exists(html_path): return "HTML not found"
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Simple injection logic for Factors/Returns
    for csv_name, var_name in [("stock_factor_data.csv", "csvData"), ("stock_returns_data.csv", "returnsCSVData")]:
        csv_path = os.path.join(current_dir, "FamaFrench", csv_name)
        if os.path.exists(csv_path):
            with open(csv_path, 'r', encoding='utf-8') as f:
                data = f.read().replace('`', '')
            injection = f"var {var_name} = `{data}`;\nPapa.parse({var_name}, {{ download: false, "
            html_content = html_content.replace(f'Papa.parse("{csv_name}", {{', injection)

    return html_content.replace('download: true,', '')


# ==========================================
# 4. Main App Interface
# ==========================================

with st.sidebar:
    st.markdown("""
    <div style='padding: 20px 0px; text-align: center; border-bottom: 1px solid #374151; margin-bottom: 20px;'>
        <h2 style='color: #F3F4F6; margin:0; letter-spacing: 1px; font-weight: 700;'>Quant</h2>
        <p style='color: #9CA3AF; font-size: 0.85em; margin-top:5px;'>Cross-Asset Strategy & Research</p>
    </div>
    """, unsafe_allow_html=True)

    selected_nav = option_menu(
        menu_title="Navigation",
        options=["Home", "Market Intelligence", "Equity Research", "Fixed Income & Vol", "Trade Portfolio", "Legal"],
        icons=["house", "globe", "search", "graph-up-arrow", "briefcase", "file-text"],
        menu_icon="compass",
        default_index=0,
        styles={
            "container": {"padding": "0!important", "background-color": "transparent"},
            "nav-link": {"font-size": "15px", "text-align": "left", "color": "#D1D5DB"},
            "nav-link-selected": {"background-color": "#2563EB", "color": "#FFFFFF"},
        }
    )

    target_page = selected_nav

    if selected_nav == "Market Intelligence":
        target_page = option_menu(
            menu_title=None,
            options=["Market Risk", "Market Breadth", "Economic Calendar", "Industry Heatmap"],
            icons=["activity", "bar-chart-line", "calendar-event", "grid-3x3"],
            styles={"nav-link": {"font-size": "14px"}}
        )

    elif selected_nav == "Equity Research":
        target_page = option_menu(
            menu_title=None,
            options=["Stock DNA", "Thematic Basket", "ETF Smart Money", "Insider Trading", "Short Squeeze", "Earnings"],
            icons=["radar", "basket", "graph-up-arrow", "people", "lightning-charge", "cash-coin"],
            styles={"nav-link": {"font-size": "14px"}}
        )

    elif selected_nav == "Fixed Income & Vol":
        target_page = option_menu(
            menu_title=None,
            options=["US Option", "HK Option", "Volume Profile", "Intraday Volatility", "HSI CBBC Ladder",
                     "Volatility Target"],
            icons=["currency-dollar", "globe-asia-australia", "bar-chart-steps", "lightning-charge",
                   "distribute-vertical", "bullseye"],
            styles={"nav-link": {"font-size": "14px"}}
        )

# ==========================================
# 5. Content Routing
# ==========================================

if target_page == "Home":
    col_main, col_profile = st.columns([0.7, 0.3], gap="large")
    with col_main:
        st.markdown("<h1 style='color:white;'>Cross-Asset Quantitative Research</h1>", unsafe_allow_html=True)
        st.markdown("<h3 style='color:#94a3b8;'>Systematic Alpha & Risk Premia Modeling</h3>", unsafe_allow_html=True)

        components.html("""
        <div class="tradingview-widget-container">
          <div class="tradingview-widget-container__widget"></div>
          <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js" async>
          {"symbols": [{"proName": "FOREXCOM:SPXUSD", "title": "S&P 500"},{"proName": "FOREXCOM:NSXUSD", "title": "US 100"},{"proName": "OANDA:XAUUSD", "title": "Gold"}],
          "showSymbolLogo": true, "colorTheme": "dark", "isTransparent": true, "displayMode": "adaptive", "locale": "en"}
          </script>
        </div>""", height=100)

        st.subheader("🧠 Week Ahead Analysis")
        st.markdown(load_weekly_analysis())

    with col_profile:
        st.markdown(f"""
            <div class="profile-card">
                <h3 style="margin-top:10px; color:#F3F4F6;">Quantitative Analyst</h3>
                <p style="color: #9CA3AF; font-size: 0.9em;">Ex-EQD Trader | Multi-Asset Specialist</p>
                <hr style="margin: 15px 0; border-top: 1px solid rgba(255,255,255,0.1);">
                <p style="text-align: left; font-size: 0.9em; line-height: 1.6; color: #e2e8f0;">
                    <b>Technical Focus:</b><br>
                    • Multi-Factor Equity Risk Premia<br>
                    • Volatility Surface & Gamma Analysis<br>
                    • HSI/NQ Intraday Mean Reversion<br>
                    • Unusual Options Activity (UOA)
                </p>
            </div>
        """, unsafe_allow_html=True)

elif target_page == "Market Risk":
    html_content, _ = get_latest_file_content("ImpliedParameters")
    if html_content: components.html(html_content, height=2200, scrolling=True)

elif target_page == "Market Breadth":
    html_content, _ = get_latest_file_content(os.path.join("MarketDashboard", "MarketBreadth"), "market_breadth_*.html")
    if html_content: components.html(html_content, height=2200, scrolling=True)

elif target_page == "Economic Calendar":
    html_content, _ = get_latest_file_content(os.path.join("MarketDashboard", "EconomicCalendar"),
                                              "calendar_report_*.html")
    if html_content: components.html(html_content, height=1200, scrolling=True)

elif target_page == "Industry Heatmap":
    html_content, _ = get_latest_file_content("MarketDashboard", "sector_etf_heatmap_*.html")
    if html_content: components.html(html_content, height=1200, scrolling=True)

elif target_page == "Stock DNA":
    st.title("🧬 Factor DNA Analysis")
    components.html(load_stock_dna_with_injection(), height=1200, scrolling=True)

elif target_page == "Thematic Basket":
    html_content, _ = get_latest_file_content("ThematicBasket", "elite_signal_dashboard_*.html")
    if html_content: components.html(html_content, height=2500, scrolling=True)

elif target_page == "ETF Smart Money":
    html_content, _ = get_latest_file_content("xETF", "ETF_Smart_Money_Report_*.html")
    if html_content: components.html(html_content, height=2000, scrolling=True)

elif target_page == "Insider Trading":
    html_content, _ = get_latest_file_content("Insider", "Insider_Trading_Report_*.html")
    if html_content: components.html(html_content, height=2000, scrolling=True)

elif target_page == "Short Squeeze":
    html_content, _ = get_latest_file_content("Short_squeeze", "Short_squeeze_*.html")
    if html_content: components.html(html_content, height=2000, scrolling=True)

elif target_page == "Earnings":
    html_content, _ = get_latest_file_content("Earnings")
    if html_content: components.html(html_content, height=2500, scrolling=True)

elif target_page == "US Option":
    html_content, _ = get_latest_file_content("Option", "option_strike_analysis_*.html")
    if html_content: components.html(html_content, height=2000, scrolling=True)

elif target_page == "HK Option":
    html_content, _ = get_latest_file_content("Option", "HK_Option_Market_Analysis_v6_*.html")
    if html_content: components.html(html_content, height=2000, scrolling=True)

elif target_page == "Volume Profile":
    html_content, _ = get_latest_file_content("VP")
    if html_content: components.html(html_content, height=1000, scrolling=True)

elif target_page == "Intraday Volatility":
    components.html(load_html_file(os.path.join("MarketDashboard", "Intraday_Volatility.html")), height=1200)

elif target_page == "HSI CBBC Ladder":
    components.html(load_html_file(os.path.join("MarketDashboard", "HSI_CBBC_Ladder.html")), height=1200)

elif target_page == "Volatility Target":
    html_content, _ = get_latest_file_content("VolTarget", "vol_tool_*.html")
    if html_content: components.html(html_content, height=1500, scrolling=True)

elif target_page == "Trade Portfolio":
    st.title("💼 Live Trade Journal & Analytics")
    html_content, _ = get_latest_file_content("Trade", "trade_record_*.html")
    if html_content: components.html(html_content, height=1200, scrolling=True)

elif target_page == "Legal":
    tab1, tab2, tab3 = st.tabs(["Disclaimer", "Privacy", "Terms"])
    with tab1:
        st.html(load_html_file(os.path.join("Legal", "disclaimer.html")))
    with tab2:
        st.html(load_html_file(os.path.join("Legal", "privacy.html")))
    with tab3:
        st.html(load_html_file(os.path.join("Legal", "terms.html")))

# ==========================================
# 6. Global Footer
# ==========================================
st.markdown("""
<div class="custom-footer">
    <p>© 2026 Portfolio Project for Quantitative Cross-Asset Research.</p>
</div>
""", unsafe_allow_html=True)