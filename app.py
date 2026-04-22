import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go

# 1. DISEÑO INSTITUCIONAL (Ancho total)
st.set_page_config(page_title="AlphaQuant Pro", layout="wide", initial_sidebar_state="expanded")
st.title("🏛️ ALPHAQUANT PRO: TERMINAL DE GESTIÓN V6")
st.markdown("---")

# 2. PANEL LATERAL (Tu centro de control global)
with st.sidebar:
    st.header("⚙️ Gestión Monetaria")
    capital = st.number_input("Capital Disponible (€)", value=10000, step=1000)
    riesgo_pct = st.slider("Riesgo por Operación (%)", 0.5, 5.0, 1.0, 0.1) / 100
    riesgo_euros = capital * riesgo_pct
    st.info(f"Riesgo fijo por operación: **{riesgo_euros:.2f} €**")
    
    st.markdown("---")
    st.header("📥 Ingesta de Datos")
    st.caption("Pega aquí tus 600 tickers del Excel (separados por coma)")
    # Lo dejamos vacío por defecto para que tú pegues tu lista gigante
    tickers_input = st.text_area("Universo de Acciones", "NVDA, AAPL, MSFT, META, TSLA, AMD, AMZN, GOOGL, PLTR, ASML")

# 3. NAVEGACIÓN POR PESTAÑAS (Fresco y Dinámico)
tab1, tab2 = st.tabs(["🌍 RADAR GLOBAL (La Matriz)", "🔬 ANÁLISIS PROFUNDO (Gráficos Interactivos)"])

# ==========================================
# PESTAÑA 1: EL ESCÁNER DE LAS 600 ACCIONES
# ==========================================
with tab1:
    st.subheader("Búsqueda de Oportunidades y Scoring")
    
    if st.button("🚀 INICIAR ESCANEO MASIVO", type="primary"):
        tickers = [t.strip().upper() for t in tickers_input.split(",") if t.strip()]
        matriz_final = []
        
        # Barra de progreso visual
        progress_text = "Escaneando Wall Street..."
        my_bar = st.progress(0, text=progress_text)
        
        for i, t in enumerate(tickers):
            # Actualizamos la barra de progreso
            percent_complete = int(((i + 1) / len(tickers)) * 100)
            my_bar.progress(percent_complete, text=f"Analizando {t} ({i+1}/{len(tickers)})")
            
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

                # Scoring de Titanio
                pts = 0
                if ret_1m > 0: pts += 20
                if ret_6m > 0: pts += 20
                if vol_hoy > (vol_medio * 1.2): pts += 20
                if per < 45: pts += 40
                elif per < 100 and ret_1m > 0.05: pts += 30
                else: pts += 5

                # Matemáticas de Excel
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
                        "ACTIVO": t,
                        "PRECIO": f"{precio:.2f} $",
                        "ACCIONES": acciones,
                        "INVERSIÓN": f"{acciones * precio:.2f} $",
                        "STOP LOSS": f"{stop_loss:.2f} $",
                        "RIESGO": f"-{riesgo_euros:.2f} €",
                        "PER": f"{per:.1f}" if per != 999 else "N/A"
                    })
            except Exception:
                continue
                
        my_bar.empty() # Borramos la barra al terminar
        
        if matriz_final:
            df = pd.DataFrame(matriz_final).sort_values(by="SCORE", ascending=False).reset_index(drop=True)
            st.success(f"✅ Análisis completado: {len(matriz_final)} activos procesados.")
            # Guardamos la tabla en memoria para la otra pestaña
            st.session_state['matriz'] = df
            st.dataframe(df, use_container_width=True, height=500)
        else:
            st.error("No se encontraron datos.")

# ==========================================
# PESTAÑA 2: GRÁFICOS Y ANÁLISIS CHULO
# ==========================================
with tab2:
    st.subheader("Gráficos Profesionales de Cotización")
    
    c1, c2 = st.columns([1, 3])
    with c1:
        # Selector dinámico: coge las acciones de la lista lateral o de la tabla
        tickers_lista = [t.strip().upper() for t in tickers_input.split(",") if t.strip()]
        ticker_elegido = st.selectbox("Selecciona un activo para analizar:", tickers_lista)
        periodo = st.radio("Marco Temporal:", ["3 Meses", "6 Meses", "1 Año", "2 Años", "5 Años"], index=2)
    
    with c2:
        if ticker_elegido:
            with st.spinner(f"Cargando gráficos pesados de {ticker_elegido}..."):
                # Traducimos el periodo para Yahoo
                p_map = {"3 Meses":"3mo", "6 Meses":"6mo", "1 Año":"1y", "2 Años":"2y", "5 Años":"5y"}
                hist_grafico = yf.Ticker(ticker_elegido).history(period=p_map[periodo])
                
                if not hist_grafico.empty:
                    # DIBUJAMOS VELAS JAPONESAS (Cosas chulas)
                    fig = go.Figure(data=[go.Candlestick(x=hist_grafico.index,
                                    open=hist_grafico['Open'],
                                    high=hist_grafico['High'],
                                    low=hist_grafico['Low'],
                                    close=hist_grafico['Close'],
                                    name="Precio")])
                    
                    fig.update_layout(
                        title=f"Acción del Precio: {ticker_elegido} ({periodo})",
                        yaxis_title='Precio ($)',
                        template='plotly_dark',
                        xaxis_rangeslider_visible=False,
                        height=600
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No hay datos históricos suficientes para dibujar el gráfico.")
