import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go

st.set_page_config(page_title="AlphaQuant Radar", layout="wide")
st.title("🎯 ALPHAQUANT: RADAR Y GRÁFICOS (Modo Excel)")
st.markdown("---")

# 1. LA BASE DE DATOS GIGANTE (Extraída de tu lista)
TICKERS = [
    "NVDA","MSFT","GOOGL","AMZN","META","AAPL","TSLA","PLTR","AMD","AVGO","SMCI","ASML","CRM","ADBE","ORCL","NOW",
    "CRWD","PANW","SNOW","DDOG","MDB","TEAM","NET","ZS","FTNT","OKTA","AI","SOUN","PATH","APP","TTD","NFLX","ANET",
    "VRT","SYM","HPE","DELL","PSTG","MRVL","ARM","BABA","IGFA","IONQ","QUBT","LAES","MSTR","COIN","MARA","RIOT",
    "CLSK","HIVE","BITF","IREN","HUT","BTBT","CIFR","WULF","GLXY","SQ","PYPL","SOFI","AFRM","UPST","HOOD","V","MA",
    "AXP","JPM","BAC","WFC","C","GS","MS","BLK","SPG","IMC","O","CME","SCHW","BX","CBM","MCP","GR","AON","ICE","COF",
    "DFS","SYF","TRV","PRU","MET","ALL","AFL","STT","BK","USB","PNC","TFC","FITB","MTB","KEY","RF","HBAN","LLY","JNJ",
    "UNH","PFE","MRK","ABBV","AMGN","VRTX","REGN","GILD","BIIB","MRNA","BNTX","ISRG","SYK","MDT","BSX","EW","DXCM",
    "ILMN","ALGN","IDXX","ZTS","ADHR","TMO","ABT","ZBH","CVS","CI","HUM","CNC","MCK","CAH","BDX","PRCT","IQV","VEEV",
    "WAT","CRL","GEHC","WMT","COST","HD","LOW","TGT","MCD","SBUX","CMG","NKE","LULU","KO","PEP","PG","EL","PM","MO",
    "KMB","CL","GIS","HSY","STZ","MNST","TJX","ROST","ULTA","KHC","MDLZ","KDP","DIS","BKNG","MAR","HLT","RCL","CCL",
    "SPOT","SHOP","MELI","PAR","AWB","FOXA","FOX","MTCH","Z","DKNG","RBLX","UTOST","MGM","NCLH","EXPE","SIRI","EA",
    "XOM","CVX","COP","SLB","HAL","BKR","EOG","OXY","MPC","VLO","PSX","FANG","HES","DVN","GE","CAT","MMM","HON","DE",
    "UNP","UPS","FDX","EMR","ETN","ITW","PH","PCAR","CMI","ROK","TTC","ARR","GWW","URI","FAST","NUE","FCX","NEM",
    "DOW","WMRS","GAP","DSH","WECL","CPRT","SN","AG","PCEX","PD","NEED","UKS","OPLD","AMTD","EXCA","EPSR","ECE","GBX",
    "PCB","REC","CIS","PGO","PSA","DLR","EQIX","VTR","AVB","ARE","SBAC","WELL","HST","CSG","PB",
    "BME:SAN","BME:BBVA","BME:CABK","BME:BKT","BME:SAB","BME:UNI","BME:ITX","BME:TEF","BME:IBE","BME:REP","BME:ELE",
    "BME:ENG","BME:NTGY","BME:GRF","BME:ROVI","BME:PHM","BME:ALM","BME:AMS","BME:FER","BME:ACS","BME:SCYR","BME:IAG",
    "BME:AENA","BME:IDR","BME:MAP","BME:GCO","BME:COL","BME:MER","BME:SLR","BME:LOG","BME:VIS","BME:MEL","BME:EBRO",
    "BME:FDR","BME:ACX","BME:ENC",
    "MC.PA","RMS.PA","OR.PA","TTE.PA","SAP.DE","BMW.DE","MBG.DE","VOW3.DE","BAS.DE","LIN","ENEL.MI","HSBA.L","AZN.L",
    "ADYEN.AS","TCEHY","JD","PDD","BIDU","NTES","NIO","XPEV","LI","BYDDY","FZKG","ELY","FXI","AOFM","EITK","UAIF",
    "TME","FUTU","BEKE","TAL","EDU","VIPS","GDS","JKS","DQ","SMIC","TSM","SONY","TM","HMC","9984.T","RELIANCE.NS",
    "HDFCBANK.NS","INFY","NUPBR","VALE","WALMEX.MX","SE","GRAB","CPNG","TV","AGH","XTN","DUM","ACR","AVAV","UAVS",
    "EHL","MTR","TXN","OCG","DLH","XLD","OSTX","THI","IKT","OSH","WMB","ATD","GHE","IWW","DSP","RBW","XTN","NER",
    "HM.DE","SAAB-B.ST","BA.L","HO.PA","AM.PA","PLR.MI"
]
# Limpiar duplicados por si acaso
TICKERS = sorted(list(set(TICKERS)))

# ==========================================
# 1. VISOR DE GRÁFICOS INSTANTÁNEO
# ==========================================
st.subheader("🔬 1. Gráficos a la carta")
c1, c2 = st.columns([1, 3])

with c1:
    ticker_grafico = st.selectbox("Busca cualquier acción de tu Excel:", TICKERS)
    periodo = st.radio("Temporalidad:", ["3 Meses", "6 Meses", "1 Año", "2 Años"], index=1)

with c2:
    if ticker_grafico:
        p_map = {"3 Meses":"3mo", "6 Meses":"6mo", "1 Año":"1y", "2 Años":"2y"}
        try:
            h = yf.Ticker(ticker_grafico).history(period=p_map[periodo])
            if not h.empty:
                fig = go.Figure(data=[go.Candlestick(x=h.index, open=h['Open'], high=h['High'], low=h['Low'], close=h['Close'])])
                fig.update_layout(title=f"{ticker_grafico} ({periodo})", template='plotly_dark', xaxis_rangeslider_visible=False, height=400, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos de Yahoo Finance para graficar este ticker.")
        except:
            st.error("Error al cargar la gráfica.")

st.markdown("---")

# ==========================================
# 2. EL RADAR (TU EXCEL)
# ==========================================
st.subheader("📊 2. El Radar de Titanio")
st.write(f"Base de datos cargada: **{len(TICKERS)} activos** listos para escanear.")

if st.button("🚀 EJECUTAR ESCÁNER COMPLETO", type="primary"):
    matriz_final = []
    my_bar = st.progress(0, text="Arrancando motores...")
    
    for i, t in enumerate(TICKERS):
        # Actualizar barra de progreso
        my_bar.progress(int(((i + 1) / len(TICKERS)) * 100), text=f"Analizando {t} ({i+1}/{len(TICKERS)})...")
        
        try:
            stock = yf.Ticker(t)
            hist = stock.history(period="6mo")
            if hist.empty: continue
            
            precio = float(hist['Close'].iloc[-1])
            info = stock.info
            per = float(info.get('trailingPE', 999)) if isinstance(info, dict) else 999
            
            ret_1m = float((precio / hist['Close'].iloc[-21]) - 1)
            ret_6m = float((precio / hist['Close'].iloc[0]) - 1)
            vol_hoy = float(hist['Volume'].iloc[-1])
            vol_medio = float(hist['Volume'].mean())

            # Puntuación exacta de Excel
            pts = 0
            if ret_1m > 0: pts += 20
            if ret_6m > 0: pts += 20
            if vol_hoy > (vol_medio * 1.2): pts += 20
            if per < 45: pts += 40
            elif per < 100 and ret_1m > 0.05: pts += 30
            else: pts += 5

            # Etiquetas
            is_whale = vol_hoy >= (vol_medio * 1.5)
            is_fenix = ret_6m < -0.10 and ret_1m > 0.05
            is_momentum = ret_6m > 0.10 and ret_1m > 0.05
            is_impulsivo = ret_1m > 0.15

            estado = "❌ ESPERAR"
            if is_fenix and pts >= 60: estado = "🦅 COMPRA FÉNIX"
            elif is_momentum and pts >= 80: estado = "🔥 COMPRA MOMENTUM"
            elif is_whale and pts >= 60: estado = "🐋 RASTRO BALLENA"
            elif is_impulsivo: estado = "⚡ RADAR IMPULSIVO"
            elif pts >= 60: estado = "💎 VIGILAR"

            # ATR y Stop Loss (Solo valor de referencia, sin riesgo en euros)
            high_low = hist['High'] - hist['Low']
            high_close = np.abs(hist['High'] - hist['Close'].shift())
            low_close = np.abs(hist['Low'] - hist['Close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            atr = float(np.max(ranges, axis=1).rolling(14).mean().iloc[-1])
            
            stop_loss = precio - (atr * 2)

            matriz_final.append({
                "SCORE": pts,
                "ESTADO": estado,
                "ACTIVO": t,
                "PRECIO": f"{precio:.2f}",
                "STOP LOSS (Nivel)": f"{stop_loss:.2f}",
                "ATR (Volatilidad)": f"{atr:.2f}",
                "PER": f"{per:.1f}" if per != 999 else "N/A"
            })
        except Exception:
            continue
            
    my_bar.empty()
    
    if matriz_final:
        df = pd.DataFrame(matriz_final).sort_values(by="SCORE", ascending=False).reset_index(drop=True)
        st.success(f"✅ Matriz generada con éxito. Se analizaron {len(matriz_final)} activos válidos.")
        
        # Filtro: Mostrar solo lo interesante arriba
        df_buenas = df[df['ESTADO'].str.contains("COMPRA|BALLENA|IMPULSIVO|VIGILAR")]
        
        if not df_buenas.empty:
            st.write("### 🎯 Oportunidades de Entrada")
            st.dataframe(df_buenas, use_container_width=True)
        else:
            st.warning("El mercado está rojo. No hay compras claras hoy.")
            
        with st.expander("Ver lista completa (Incluyendo las de '❌ ESPERAR')"):
            st.dataframe(df, use_container_width=True)
