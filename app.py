import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np

st.set_page_config(page_title="AlphaQuant Dashboard", layout="wide", initial_sidebar_state="expanded")
st.title("📊 ALPHAQUANT: TERMINAL V5 (Matemática Exacta)")
st.markdown("---")

with st.sidebar:
    st.header("⚙️ Gestión Monetaria")
    capital = st.number_input("Capital Disponible (€)", value=5000, step=500)
    # AÑADIDO: Ahora controlas el riesgo exacto como en Excel
    riesgo_pct = st.number_input("Riesgo Máximo por Operación (%)", value=2.0, step=0.5) / 100
    riesgo_euros = capital * riesgo_pct
    st.info(f"Si la operación sale mal, perderás máximo: **{riesgo_euros:.2f} €**")
    
    lista_defecto = "NVDA, AAPL, MSFT, META, GOOGL, AMZN, TSLA, PLTR, AVGO, ASML"
    tickers_input = st.text_area("Radar de Vigilancia", lista_defecto)

if st.button("🚀 EJECUTAR MOTOR QUANTITATIVO", type="primary"):
    tickers = [t.strip().upper() for t in tickers_input.split(",")]
    matriz_final = []
    historicos = {}
    
    with st.spinner('Sincronizando algoritmos con Wall Street...'):
        for t in tickers:
            try:
                stock = yf.Ticker(t)
                hist = stock.history(period="6mo")
                if hist.empty: continue
                historicos[t] = hist
                
                precio = float(hist['Close'].iloc[-1])
                
                # 1. FILTRO DE TITANIO
                pts = 0
                ret_1m = float((precio / hist['Close'].iloc[-21]) - 1)
                ret_6m = float((precio / hist['Close'].iloc[0]) - 1)
                vol_hoy = float(hist['Volume'].iloc[-1])
                vol_medio = float(hist['Volume'].mean())
                info = stock.info
                per = float(info.get('trailingPE', 999)) if isinstance(info, dict) else 999

                if ret_1m > 0: pts += 20
                if ret_6m > 0: pts += 20
                if vol_hoy > (vol_medio * 1.2): pts += 20
                if per < 45: pts += 40
                elif per < 100 and ret_1m > 0.05: pts += 30
                else: pts += 5

                estado = "🔥 COMPRA" if pts >= 80 else "💎 VIGILAR" if pts >= 50 else "❌ ESPERAR"

                # 2. MATEMÁTICAS EXACTAS DEL EXCEL
                # ATR Real (Cálculo profesional de Wall Street)
                high_low = hist['High'] - hist['Low']
                high_close = np.abs(hist['High'] - hist['Close'].shift())
                low_close = np.abs(hist['Low'] - hist['Close'].shift())
                ranges = pd.concat([high_low, high_close, low_close], axis=1)
                atr = float(np.max(ranges, axis=1).rolling(14).mean().iloc[-1])
                
                # Fórmulas de tu Excel original
                distancia_stop = atr * 2
                stop_loss = precio - distancia_stop
                
                # ACCIONES FRACCIONADAS (Se acabaron los ceros)
                # Acciones = Riesgo Total / Distancia al Stop
                acciones = round(riesgo_euros / distancia_stop, 2)
                inversion = acciones * precio
                
                ratio_tp = 3 if pts >= 80 else 2
                take_profit = precio + (distancia_stop * ratio_tp)
                beneficio_esp = acciones * (take_profit - precio)

                matriz_final.append({
                    "SCORE": pts,
                    "ACTIVO": t,
                    "PRECIO": f"{precio:.2f} $",
                    "ESTADO": estado,
                    "ATR": f"{atr:.2f} $",
                    "ACCIONES": acciones, 
                    "INVERSIÓN": f"{inversion:.2f} $",
                    "STOP LOSS": f"{stop_loss:.2f} $",
                    "TAKE PROFIT": f"{take_profit:.2f} $",
                    "RIESGO": f"-{riesgo_euros:.2f} $",
                    "BENEFICIO ESP.": f"+{beneficio_esp:.2f} $"
                })
            except Exception as e:
                continue

    if matriz_final:
        df = pd.DataFrame(matriz_final).sort_values(by="SCORE", ascending=False).reset_index(drop=True)
        
        st.success("✅ Matriz Completada. Matemáticas 100% alineadas con Excel.")
        st.dataframe(df, use_container_width=True)
        
        top_ticker = df.iloc[0]['ACTIVO']
        st.subheader(f"📈 Gráfico de la Medalla de Oro: {top_ticker}")
        st.line_chart(historicos[top_ticker]['Close'], color="#00FF41")
    else:
        st.error("No se han podido procesar los datos.")
