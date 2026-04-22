import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np

st.set_page_config(page_title="AlphaQuant Titanium", layout="wide")
st.title("🛡️ RADAR DE TITANIO: MATRIZ COMPLETA")
st.markdown("---")

with st.sidebar:
    st.header("⚙️ Configuración")
    capital = st.number_input("Tu Capital (€)", value=5000)
    lista_defecto = "NVDA, AAPL, MSFT, META, GOOGL, AMZN, TSLA, PLTR, AVGO, ASML"
    tickers_input = st.text_area("Lista de Vigilancia", lista_defecto)

if st.button("🚀 EJECUTAR ANÁLISIS DE TITANIO", type="primary"):
    tickers = [t.strip().upper() for t in tickers_input.split(",")]
    matriz_final = []
    
    with st.spinner('Escaneando mercado...'):
        for t in tickers:
            try:
                stock = yf.Ticker(t)
                hist = stock.history(period="6mo")
                info = stock.info
                if hist.empty: continue
                
                precio = hist['Close'].iloc[-1]
                pts = 0
                ret_1m = (precio / hist['Close'].iloc[-21]) - 1
                ret_6m = (precio / hist['Close'].iloc[0]) - 1
                vol_hoy = hist['Volume'].iloc[-1]
                vol_medio = hist['Volume'].mean()
                per = info.get('trailingPE', 999)

                if ret_1m > 0: pts += 20
                if ret_6m > 0: pts += 20
                if vol_hoy > (vol_medio * 1.2): pts += 20
                
                if per < 45: pts += 40
                elif per < 100 and ret_1m > 0.05: pts += 30
                else: pts += 5

                atr = (hist['High'] - hist['Low']).tail(14).mean()
                stop = precio - (atr * 2)
                kelly_frac = 0.15 if pts >= 80 else 0.08 if pts >= 50 else 0
                acciones = int((capital * kelly_frac) / precio)

                matriz_final.append({
                    "SCORE": pts,
                    "ACTIVO": t,
                    "PRECIO": f"{precio:.2f}$",
                    "ESTADO": "🔥 COMPRA" if pts >= 80 else "💎 VIGILAR" if pts >= 50 else "❌ ESPERAR",
                    "ACCIONES": acciones,
                    "STOP LOSS": f"{stop:.2f}$"
                })
            except: continue

    if matriz_final:
        df = pd.DataFrame(matriz_final).sort_values(by="SCORE", ascending=False).reset_index(drop=True)
        st.success("✅ Escaneo completado con éxito.")
        st.table(df)
    else:
        st.error("No se han podido obtener datos.")
