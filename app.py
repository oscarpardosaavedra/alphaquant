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
    errores = []
    
    with st.spinner('Escaneando mercado a nivel global...'):
        for t in tickers:
            try:
                stock = yf.Ticker(t)
                hist = stock.history(period="6mo")
                
                if hist.empty:
                    errores.append(f"Sin historial para {t}")
                    continue
                
                precio = float(hist['Close'].iloc[-1])
                pts = 0
                ret_1m = float((precio / hist['Close'].iloc[-21]) - 1)
                ret_6m = float((precio / hist['Close'].iloc[0]) - 1)
                vol_hoy = float(hist['Volume'].iloc[-1])
                vol_medio = float(hist['Volume'].mean())
                
                # Extracción segura del PER
                info = stock.info
                per = 999
                if isinstance(info, dict) and 'trailingPE' in info:
                    per = float(info['trailingPE'])

                # Reglas de Puntuación
                if ret_1m > 0: pts += 20
                if ret_6m > 0: pts += 20
                if vol_hoy > (vol_medio * 1.2): pts += 20
                
                if per < 45: pts += 40
                elif per < 100 and ret_1m > 0.05: pts += 30
                else: pts += 5

                # Riesgo
                atr = float((hist['High'] - hist['Low']).tail(14).mean())
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
            except Exception as e:
                errores.append(f"Error técnico en {t}: {str(e)}")
                continue

    if matriz_final:
        df = pd.DataFrame(matriz_final).sort_values(by="SCORE", ascending=False).reset_index(drop=True)
        st.success("✅ Escaneo completado con éxito.")
        
        # Podio
        st.subheader("🥇 PODIO DE INVERSIÓN (Top Scoring)")
        cols = st.columns(3)
        for i in range(min(3, len(df))):
            with cols[i]:
                medalla = ["🥇", "🥈", "🥉"][i]
                st.metric(f"{medalla} {df.iloc[i]['ACTIVO']}", df.iloc[i]['SCORE'], f"Sugerido: {df.iloc[i]['ACCIONES']} acc")
        
        st.markdown("---")
        st.table(df)
    else:
        st.error("⚠️ No se pudo escanear el mercado.")
        
    # Chivato de Errores
    if errores:
        with st.expander("🛠️ Ver registro de errores internos"):
            for err in errores:
                st.write(err)
