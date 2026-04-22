import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go

st.set_page_config(page_title="AlphaQuant Maestra", layout="wide", initial_sidebar_state="expanded")
st.title("🧙‍♂️ ALPHAQUANT V7: LA MATRIZ MAESTRA")
st.markdown("---")

# 1. BASE DE DATOS INTERNA (Añade aquí tus 600 si quieres)
TICKERS_PREDEFINIDOS = [
    "NVDA", "AAPL", "MSFT", "META", "AMZN", "GOOGL", "TSLA", "PLTR", "ASML", "AVGO",
    "AMD", "NFLX", "CRM", "INTC", "QCOM", "TXN", "ADBE", "CSCO", "IBM", "ORCL",
    "ENG.MC", "IBE.MC", "REP.MC", "SAN.MC", "ITX.MC", "BAS.DE", "SAP.DE", "VOW3.DE"
]

with st.sidebar:
    st.header("⚙️ Gestión Monetaria")
    capital = st.number_input("Capital Disponible (€)", value=10000, step=1000)
    riesgo_pct = st.slider("Riesgo por Operación (%)", 0.5, 5.0, 1.0, 0.1) / 100
    riesgo_euros = capital * riesgo_pct
    st.info(f"Riesgo fijo: **{riesgo_euros:.2f} €** por operación")
    
    st.markdown("---")
    st.header("📥 Universo de Acciones")
    # Desplegable inteligente (Multiselect)
    tickers_seleccionados = st.multiselect(
        "Elige tus tickers (Puedes escribir para buscar):",
        options=TICKERS_PREDEFINIDOS,
        default=["NVDA", "PLTR", "META", "ASML", "TSLA"]
    )
    
    # Opción para añadir tickers raros a mano
    ticker_extra = st.text_input("¿Falta alguno? Añádelo aquí (ej: COIN):")
    if ticker_extra:
        tickers_seleccionados.append(ticker_extra.upper())

# 2. PESTAÑAS DEL SISTEMA
tab1, tab2, tab3 = st.tabs(["🎯 SEÑALES DE COMPRA (Filtrado)", "🌍 MATRIZ COMPLETA", "🔬 GRÁFICOS INTERACTIVOS"])

matriz_final = []
historicos = {}

# 3. MOTOR DE CÁLCULO
if st.button("🚀 CAZAR ALPHA (Ejecutar Escáner)", type="primary"):
    if not tickers_seleccionados:
        st.warning("Selecciona al menos un ticker en el panel lateral.")
    else:
        progress_text = "Buscando Gacelas, Fénix y Ballenas..."
        my_bar = st.progress(0, text=progress_text)
        
        for i, t in enumerate(tickers_seleccionados):
            my_bar.progress(int(((i + 1) / len(tickers_seleccionados)) * 100), text=f"Analizando {t}...")
            
            try:
                stock = yf.Ticker(t)
                hist = stock.history(period="6mo")
                if hist.empty: continue
                historicos[t] = hist
                
                precio = float(hist['Close'].iloc[-1])
                info = stock.info
                per = float(info.get('trailingPE', 999)) if isinstance(info, dict) else 999
                
                ret_1m = float((precio / hist['Close'].iloc[-21]) - 1)
                ret_6m = float((precio / hist['Close'].iloc[0]) - 1)
                vol_hoy = float(hist['Volume'].iloc[-1])
                vol_medio = float(hist['Volume'].mean())

                # --- LÓGICA DE EXCEL EXACTA (Categorías Mágicas) ---
                pts = 0
                if ret_1m > 0: pts += 20
                if ret_6m > 0: pts += 20
                if vol_hoy > (vol_medio * 1.2): pts += 20
                if per < 45: pts += 40
                elif per < 100 and ret_1m > 0.05: pts += 30
                else: pts += 5

                # Detección de Patrones
                is_whale = vol_hoy >= (vol_medio * 1.5)
                is_fenix = ret_6m < -0.10 and ret_1m > 0.05
                is_momentum = ret_6m > 0.10 and ret_1m > 0.05
                is_impulsivo = ret_1m > 0.15

                veredicto = "❌ ESPERAR"
                if is_fenix and pts >= 60: veredicto = "🦅 COMPRA FÉNIX"
                elif is_momentum and pts >= 80: veredicto = "🔥 COMPRA MOMENTUM"
                elif is_whale and pts >= 60: veredicto = "🐋 RASTRO BALLENA"
                elif is_impulsivo: veredicto = "⚡ RADAR IMPULSIVO"
                elif pts >= 60: veredicto = "💎 VIGILAR"

                # Gestión de Riesgo Kelly/ATR
                high_low = hist['High'] - hist['Low']
                high_close = np.abs(hist['High'] - hist['Close'].shift())
                low_close = np.abs(hist['Low'] - hist['Close'].shift())
                ranges = pd.concat([high_low, high_close, low_close], axis=1)
                atr = float(np.max(ranges, axis=1).rolling(14).mean().iloc[-1])
                
                distancia_stop = atr * 2
                stop_loss = precio - distancia_stop
                acciones = round(riesgo_euros / distancia_stop, 2)
                
                if acciones > 0:
                    matriz_final.append({
                        "SCORE": pts,
                        "ESTADO": veredicto,
                        "ACTIVO": t,
                        "PRECIO": f"{precio:.2f} $",
                        "ACCIONES": acciones,
                        "INVERSIÓN": f"{acciones * precio:.2f} $",
                        "STOP LOSS": f"{stop_loss:.2f} $",
                        "PER": f"{per:.1f}" if per != 999 else "N/A"
                    })
            except Exception:
                continue
                
        my_bar.empty()
        
        if matriz_final:
            df = pd.DataFrame(matriz_final).sort_values(by="SCORE", ascending=False).reset_index(drop=True)
            st.session_state['df_completo'] = df
            st.session_state['historicos'] = historicos

# --- RENDERIZADO DE LAS PESTAÑAS ---
if 'df_completo' in st.session_state:
    df = st.session_state['df_completo']
    
    with tab1:
        st.subheader("🎯 Oportunidades Listas para Ejecutar")
        st.write("Aquí solo ves lo que tu Excel mandaría comprar (Fénix, Momentum, Ballena o Impulsivo). Basura fuera.")
        # Filtramos la tabla
        df_compras = df[df['ESTADO'].str.contains("COMPRA|BALLENA|IMPULSIVO")]
        if not df_compras.empty:
            st.dataframe(df_compras, use_container_width=True)
        else:
            st.info("Hoy no hay señales claras de compra. El mercado manda esperar.")
            
    with tab2:
        st.subheader("🌍 Matriz Completa (Todas las analizadas)")
        st.dataframe(df, use_container_width=True)

if 'historicos' in st.session_state:
    with tab3:
        st.subheader("🔬 Sala de Gráficos de Wall Street")
        c1, c2 = st.columns([1, 3])
        with c1:
            ticker_grafico = st.selectbox("Selecciona un activo analizado:", list(st.session_state['historicos'].keys()))
        with c2:
            if ticker_grafico:
                h = st.session_state['historicos'][ticker_grafico]
                fig = go.Figure(data=[go.Candlestick(x=h.index, open=h['Open'], high=h['High'], low=h['Low'], close=h['Close'])])
                fig.update_layout(title=f"Acción del Precio: {ticker_grafico} (6 Meses)", template='plotly_dark', xaxis_rangeslider_visible=False, height=500)
                st.plotly_chart(fig, use_container_width=True)
