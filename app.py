import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np

# 1. ESTÉTICA DE TERMINAL INSTITUCIONAL
st.set_page_config(page_title="AlphaQuant Dashboard", layout="wide", initial_sidebar_state="expanded")
st.title("📊 ALPHAQUANT: TERMINAL DE GESTIÓN V4")
st.markdown("---")

# 2. PANEL DE CONTROL
with st.sidebar:
    st.header("⚙️ Parámetros de Riesgo")
    capital = st.number_input("Capital Disponible (€)", value=5000, step=500)
    lista_defecto = "NVDA, AAPL, MSFT, META, GOOGL, AMZN, TSLA, PLTR, AVGO, ASML"
    tickers_input = st.text_area("Radar de Vigilancia", lista_defecto)

# 3. EL MOTOR QUANT
if st.button("🚀 EJECUTAR ESCÁNER COMPLETO", type="primary"):
    tickers = [t.strip().upper() for t in tickers_input.split(",")]
    matriz_final = []
    historicos = {} # Memoria para los gráficos
    
    with st.spinner('Conectando a Wall Street y procesando gráficos...'):
        for t in tickers:
            try:
                stock = yf.Ticker(t)
                hist = stock.history(period="6mo")
                if hist.empty: continue
                
                historicos[t] = hist # Guardamos el historial de la empresa
                
                precio = float(hist['Close'].iloc[-1])
                pts = 0
                ret_1m = float((precio / hist['Close'].iloc[-21]) - 1)
                ret_6m = float((precio / hist['Close'].iloc[0]) - 1)
                vol_hoy = float(hist['Volume'].iloc[-1])
                vol_medio = float(hist['Volume'].mean())
                
                info = stock.info
                per = 999
                if isinstance(info, dict) and 'trailingPE' in info: per = float(info['trailingPE'])

                # Filtro de Titanio
                if ret_1m > 0: pts += 20
                if ret_6m > 0: pts += 20
                if vol_hoy > (vol_medio * 1.2): pts += 20
                if per < 45: pts += 40
                elif per < 100 and ret_1m > 0.05: pts += 30
                else: pts += 5

                # Cálculo ATR y Kelly (Como en Excel)
                atr = float((hist['High'] - hist['Low']).tail(14).mean())
                stop = precio - (atr * 2)
                ratio_tp = 3 if pts >= 80 else 2 # Ratio 1:3 para las mejores
                take_profit = precio + (atr * 2 * ratio_tp)
                
                kelly_frac = 0.15 if pts >= 80 else 0.08 if pts >= 50 else 0
                acciones = int((capital * kelly_frac) / precio)

                # Matemáticas de Cartera
                inversion = acciones * precio
                riesgo = acciones * (precio - stop)
                beneficio = acciones * (take_profit - precio)

                matriz_final.append({
                    "SCORE": pts,
                    "ACTIVO": t,
                    "PRECIO": f"{precio:.2f} $",
                    "ESTADO": "🔥 COMPRA" if pts >= 80 else "💎 VIGILAR" if pts >= 50 else "❌ ESPERAR",
                    "ACCIONES": acciones,
                    "INVERSIÓN": f"{inversion:.2f} $",
                    "STOP LOSS": f"{stop:.2f} $",
                    "TAKE PROFIT": f"{take_profit:.2f} $",
                    "RIESGO": f"-{riesgo:.2f} $" if acciones > 0 else "0.00 $",
                    "BENEFICIO ESP.": f"+{beneficio:.2f} $" if acciones > 0 else "0.00 $"
                })
            except Exception as e:
                continue

    # 4. LA MAGIA VISUAL
    if matriz_final:
        df = pd.DataFrame(matriz_final).sort_values(by="SCORE", ascending=False).reset_index(drop=True)
        
        # EL GRÁFICO (Cogemos la número 1 del ranking)
        top_ticker = df.iloc[0]['ACTIVO']
        st.success(f"🎯 Escaneo completado. La Gacela Principal hoy es **{top_ticker}**.")
        
        st.subheader(f"📈 Gráfico de Cotización (Últimos 6 meses): {top_ticker}")
        # Dibujamos un gráfico interactivo nativo de Streamlit
        st.area_chart(historicos[top_ticker]['Close'], color="#00FF41")
        
        st.markdown("---")
        
        # LA MATRIZ ESTILO EXCEL (Interactiva)
        st.subheader("📋 Matriz Cuantitativa y Gestión Monetaria")
        st.dataframe(df, use_container_width=True)
        
        st.markdown("---")
        
        # PANELES DE RESUMEN MACRO (Sumatorio de la cartera)
        st.subheader("⚖️ Balance Global de la Operación")
        # Sumamos los valores quitando los símbolos de dólar
        total_inv = sum([float(str(r['INVERSIÓN']).replace(' $','')) for r in matriz_final])
        total_riesgo = sum([float(str(r['RIESGO']).replace(' $','').replace('-','')) for r in matriz_final])
        total_ben = sum([float(str(r['BENEFICIO ESP.']).replace(' $','').replace('+','')) for r in matriz_final])
        
        c1, c2, c3 = st.columns(3)
        c1.metric("💰 CAPITAL TOTAL DESPLEGADO", f"{total_inv:.2f} $")
        c2.metric("⚠️ RIESGO MÁXIMO (STOP LOSS)", f"-{total_riesgo:.2f} $")
        c3.metric("🎯 BENEFICIO POTENCIAL", f"+{total_ben:.2f} $")
        
    else:
        st.error("⚠️ El mercado está cerrado o no hay datos disponibles.")
