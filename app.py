import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import re

st.set_page_config(page_title="AlphaQuant Replica", layout="wide")
st.title("📊 ALPHAQUANT: RÉPLICA EXACTA DEL EXCEL")
st.markdown("---")

with st.sidebar:
    st.header("1️⃣ Tu Cartera")
    capital = st.number_input("Capital Disponible (€)", value=10000, step=1000)
    riesgo_euros = st.number_input("Riesgo Fijo por Operación (€)", value=100, help="Cuánto estás dispuesto a perder si salta el Stop Loss")
    
    st.markdown("---")
    st.header("2️⃣ Pegar Tickers")
    st.caption("Ve a tu Excel, copia la columna entera con tus 600 tickers y pégala aquí dentro:")
    
    # Text area gigante ideal para copiar/pegar columnas de Excel
    default_tickers = "NVDA\nAAPL\nMSFT\nMETA\nAMZN\nGOOGL\nTSLA\nPLTR\nASML\nAVGO"
    tickers_input = st.text_area("Universo de Acciones", default_tickers, height=300)

if st.button("🚀 EJECUTAR RADAR (Igual que en Excel)", type="primary"):
    # Separar por saltos de línea, comas o espacios (A prueba de balas al pegar desde Excel)
    tickers_raw = re.split(r'[,\s]+', tickers_input.strip())
    tickers = list(set([t.upper() for t in tickers_raw if t])) 
    
    matriz_final = []
    historicos = {}
    
    my_bar = st.progress(0, text=f"Escaneando {len(tickers)} activos...")
    
    for i, t in enumerate(tickers):
        my_bar.progress(int(((i + 1) / len(tickers)) * 100), text=f"Analizando {t}...")
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

            # Scoring exacto de tu sistema
            pts = 0
            if ret_1m > 0: pts += 20
            if ret_6m > 0: pts += 20
            if vol_hoy > (vol_medio * 1.2): pts += 20
            if per < 45: pts += 40
            elif per < 100 and ret_1m > 0.05: pts += 30
            else: pts += 5

            # Etiquetas del Excel (Magia Pura)
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

            # Gestión simple (Réplica de las columnas de tu Excel)
            high_low = hist['High'] - hist['Low']
            high_close = np.abs(hist['High'] - hist['Close'].shift())
            low_close = np.abs(hist['Low'] - hist['Close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            atr = float(np.max(ranges, axis=1).rolling(14).mean().iloc[-1])
            
            distancia_stop = atr * 2
            stop_loss = precio - distancia_stop
            acciones = round(riesgo_euros / distancia_stop, 2) if distancia_stop > 0 else 0
            
            inversion = acciones * precio
            ratio_tp = 3 if pts >= 80 else 2
            take_profit = precio + (distancia_stop * ratio_tp)
            beneficio = acciones * (take_profit - precio)
            
            matriz_final.append({
                "SCORE": pts,
                "ESTADO": estado,
                "ACTIVO": t,
                "PRECIO": f"{precio:.2f} $",
                "ACCIONES": acciones,
                "INVERSIÓN": f"{inversion:.2f} $",
                "STOP LOSS": f"{stop_loss:.2f} $",
                "TAKE PROFIT": f"{take_profit:.2f} $",
                "RIESGO": f"-{riesgo_euros:.2f} $",
                "BENEFICIO ESP.": f"+{beneficio:.2f} $"
            })
        except Exception:
            continue
            
    my_bar.empty() # Quitar barra al terminar
    
    if matriz_final:
        df = pd.DataFrame(matriz_final).sort_values(by="SCORE", ascending=False).reset_index(drop=True)
        
        # 1. RESUMEN GLOBAL DE LA CARTERA (Lo que querías ver)
        st.subheader("⚖️ Balance Global de la Operación")
        # Sumamos solo las que el sistema nos dice de COMPRAR o VIGILAR
        df_activas = df[df['ESTADO'].str.contains("COMPRA|BALLENA|IMPULSIVO")]
        
        total_inv = sum([float(str(r).replace(' $','')) for r in df_activas['INVERSIÓN']]) if not df_activas.empty else 0
        total_riesgo = sum([float(str(r).replace(' $','').replace('-','')) for r in df_activas['RIESGO']]) if not df_activas.empty else 0
        total_ben = sum([float(str(r).replace(' $','').replace('+','')) for r in df_activas['BENEFICIO ESP.']]) if not df_activas.empty else 0
        
        c1, c2, c3 = st.columns(3)
        c1.metric("💰 CAPITAL TOTAL INVERTIDO", f"{total_inv:.2f} $")
        c2.metric("⚠️ RIESGO TOTAL ASUMIDO", f"-{total_riesgo:.2f} $")
        c3.metric("🎯 BENEFICIO POTENCIAL", f"+{total_ben:.2f} $")
        st.markdown("*(Los totales suman automáticamente solo las acciones con señal de Compra, Ballena o Impulso)*")
        st.markdown("---")
        
        # 2. TABLA PRINCIPAL (Filtro exacto de tu Excel)
        st.subheader("📋 MATRIZ DE COMPRAS (El Radar)")
        df_buenas = df[df['ESTADO'].str.contains("COMPRA|BALLENA|IMPULSIVO|VIGILAR")]
        if not df_buenas.empty:
            st.dataframe(df_buenas, use_container_width=True)
        else:
            st.warning("Hoy el mercado manda estar quieto. No hay señales claras.")
            
        with st.expander("Ver tabla completa (Incluidas las de '❌ ESPERAR')"):
            st.dataframe(df, use_container_width=True)
            
        st.markdown("---")
        
        # 3. GRÁFICAS INTEGRADAS
        st.subheader("🔬 Gráficas Interactivas")
        tickers_validos = df['ACTIVO'].tolist()
        ticker_elegido = st.selectbox("Selecciona un activo de la tabla para ver su gráfica:", tickers_validos)
        
        if ticker_elegido:
            h = historicos[ticker_elegido]
            fig = go.Figure(data=[go.Candlestick(x=h.index, open=h['Open'], high=h['High'], low=h['Low'], close=h['Close'])])
            fig.update_layout(title=f"Acción del Precio: {ticker_elegido} (Últimos 6 meses)", template='plotly_dark', xaxis_rangeslider_visible=False, height=550)
            st.plotly_chart(fig, use_container_width=True)
