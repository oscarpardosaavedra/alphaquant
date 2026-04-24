import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import datetime
import pytz
import gspread
from google.oauth2.service_account import Credentials
import time
import requests

# ==========================================
# 1. CONFIGURACIÓN Y CONEXIÓN DB (BASE DE DATOS)
# ==========================================
st.set_page_config(page_title="Alphaquant", page_icon="📈", layout="wide")

# ---> MEMORIA PERSISTENTE PARA QUE NO SE BORRE EL RADAR NI AUDITORÍA <---
if 'resultados_radar' not in st.session_state: st.session_state.resultados_radar = None
if 'resultados_auditoria' not in st.session_state: st.session_state.resultados_auditoria = None

def conectar_db():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_dict = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open("Alphaquant_DB").worksheet("Trofeos")
        return sheet
    except Exception as e:
        st.error(f"⚠️ Error de conexión con la base de datos. Revisa tus Secrets. Detalle: {e}")
        return None

# ==========================================
# 2. ESTILOS CSS (TITANIUM DESIGN)
# ==========================================
st.markdown("""
<style>
    [data-testid="stMetric"] { 
        background-color: #f8f9fa; 
        border-radius: 10px; 
        padding: 15px; 
        box-shadow: 0 4px 6px rgba(0,0,0,0.05); 
    }
    .stDataFrame th, [data-testid="stDataFrame"] th { 
        font-weight: 900 !important; 
        color: #073763 !important; 
        text-transform: uppercase; 
    }
    .stat-badge {
        background: #f0f4f8; padding: 4px 8px; border-radius: 4px; 
        font-size: 11px; font-weight: bold; color: #34495e; 
        margin-right: 5px; border: 1px solid #e1e8ed; cursor: help;
    }
    .stat-badge:hover { background: #e1e8ed; }
    
    /* ---> TEXTO "EVALUANDO" MÁS GRANDE Y VISIBLE (SIN TOCAR LA BARRA) <--- */
    [data-testid="stProgress"] p {
        font-size: 18px !important;
        font-weight: bold !important;
        margin-bottom: 5px !important;
    }
    [data-testid="stProgress"] code {
        font-size: 18px !important;
        color: #073763 !important;
        background-color: transparent !important;
        font-weight: 900 !important;
    }
</style>
""", unsafe_allow_html=True)

# Banner Principal
st.markdown("""
<div style="background: linear-gradient(135deg, #0f2027 0%, #203a43 50%, #2c5364 100%); padding: 25px; border-radius: 12px; text-align: center; margin-bottom: 30px; box-shadow: 0 4px 8px rgba(0,0,0,0.1);">
    <h1 style="color: white; margin: 0; font-size: 2.8em; font-family: 'Segoe UI', Tahoma, sans-serif; letter-spacing: 2px;">📈 ALPHAQUANT</h1>
</div>
""", unsafe_allow_html=True)

# ==========================================
# 3. BASE DE DATOS DE TICKERS (DICCIONARIO COMPLETO)
# ==========================================
tickers_nombres = {
    "AGH": "Powerus", "XTND": "Xtend", "UMAC": "Unusual Mac", "RCAT": "Red Cat",
    "AVAV": "AeroViron", "UAVS": "AgEagle", "EH": "EHang", "LMT": "Lockheed",
    "RTX": "Raytheon", "NOC": "Northrop", "GD": "GenDynamics", "LHX": "L3Harris",
    "LDOS": "Leidos", "TXT": "Textron", "HII": "Huntington", "KTOS": "Kratos",
    "HWM": "Howmet", "BA": "Boeing", "TDG": "TransDigm", "HEI": "Heico",
    "WWD": "Woodward", "SPR": "SpiritAero", "BWXT": "BWX Tech", "NNE": "Nano Nuc",
    "RHM.DE": "Rheinmetall", "SAAB-B.ST": "Saab", "BA.L": "BAE Sys", "HO.PA": "Thales",
    "AM.PA": "Dassault", "PLR.MI": "Leonardo", "ESLT": "Elbit", "NVDA": "NVIDIA",
    "MSFT": "Microsoft", "GOOGL": "Alphabet", "AMZN": "Amazon", "META": "Meta",
    "AAPL": "Apple", "TSLA": "Tesla", "PLTR": "Palantir", "AMD": "AdvMicro",
    "AVGO": "Broadcom", "SMCI": "SuperMicro", "ASML": "ASML", "CRM": "Salesforce",
    "ADBE": "Adobe", "ORCL": "Oracle", "NOW": "ServiceNow", "CRWD": "CrowdStrike",
    "PANW": "PaloAlto", "SNOW": "Snowflake", "DDOG": "Datadog", "MDB": "MongoDB",
    "TEAM": "Atlassian", "NET": "Cloudflare", "ZS": "Zscaler", "FTNT": "Fortinet",
    "OKTA": "Okta", "AI": "C3.ai", "SOUN": "SoundHound", "PATH": "UiPath",
    "APP": "AppLovin", "TTD": "Trade Desk", "NFLX": "Netflix", "ANET": "Arista",
    "VRT": "Vertiv", "SYM": "Symbotic", "HPE": "HewlettP", "DELL": "Dell",
    "PSTG": "PureStor", "MRVL": "Marvell", "ARM": "ARM Hold", "BBAI": "BigBear",
    "GFAI": "Guardforce", "IONQ": "IONQ", "QBTS": "D-Wave", "RGTI": "Rigetti",
    "QUBT": "QuantumComp", "LAES": "Sealsq", "MSTR": "MicroStrat", "COIN": "Coinbase",
    "MARA": "Mara", "RIOT": "Riot", "CLSK": "CleanSpark", "HIVE": "Hive Dig",
    "BITF": "Bitfarms", "IREN": "Iris Ene", "HUT": "Hut 8", "BTBT": "Bit Dig",
    "CIFR": "Cipher", "WULF": "TeraWulf", "GLXY": "Galaxy", "SQ": "Block",
    "PYPL": "PayPal", "SOFI": "SoFi", "AFRM": "Affirm", "UPST": "Upstart",
    "HOOD": "Robinhood", "V": "Visa", "MA": "Mastercard", "AXP": "Amex",
    "JPM": "JPMorgan", "BAC": "BankAm", "WFC": "WellsFargo", "C": "Citigroup",
    "GS": "Goldman", "MS": "MorganStan", "BLK": "BlackRock", "SPGI": "SP Global",
    "MCO": "Moodys", "CME": "CME Group", "SCHW": "Schwab", "BX": "Blackstone",
    "CB": "Chubb", "MMC": "MarshMc", "PGR": "Progressive", "AON": "Aon",
    "ICE": "Intercont", "COF": "CapitalOne", "DFS": "Discover", "SYF": "Synchrony",
    "TRV": "Travelers", "PRU": "Prudential", "MET": "MetLife", "ALL": "Allstate",
    "AFL": "Aflac", "STT": "StateSt", "BK": "BankNY", "USB": "USBancorp",
    "PNC": "PNC Fin", "TFC": "Truist", "FITB": "FifthThird", "MTB": "MT Bank",
    "KEY": "KeyCorp", "RF": "Regions", "HBAN": "Huntington", "LLY": "Eli Lilly",
    "JNJ": "J&J", "UNH": "UnitedHealth", "PFE": "Pfizer", "MRK": "Merck",
    "ABBV": "AbbVie", "AMGN": "Amgen", "VRTX": "Vertex", "REGN": "Regeneron",
    "GILD": "Gilead", "BIIB": "Biogen", "MRNA": "Moderna", "BNTX": "BioNTech",
    "ISRG": "Intuitive", "SYK": "Stryker", "MDT": "Medtronic", "BSX": "BostonSci",
    "EW": "Edwards", "DXCM": "DexCom", "ILMN": "Illumina", "ALGN": "Align",
    "IDXX": "IDEXX", "ZTS": "Zoetis", "A": "Agilent", "DHR": "Danaher",
    "TMO": "ThermoFish", "ABT": "Abbott", "ZBH": "Zimmer", "CVS": "CVS",
    "CI": "Cigna", "HUM": "Humana", "CNC": "Centene", "MCK": "McKesson",
    "CAH": "Cardinal", "BDX": "Becton", "PRCT": "Procept", "IQV": "IQVIA",
    "VEEV": "Veeva", "WAT": "Waters", "CRL": "CharlesRiv", "GEHC": "GEHealth",
    "WMT": "Walmart", "COST": "Costco", "HD": "HomeDepot", "LOW": "Lowe's",
    "TGT": "Target", "MCD": "McDonalds", "SBUX": "Starbucks", "CMG": "Chipotle",
    "NKE": "Nike", "LULU": "Lululemon", "KO": "CocaCola", "PEP": "PepsiCo",
    "PG": "Procter", "EL": "EsteeLauder", "PM": "PhilipMor", "MO": "Altria",
    "KMB": "Kimberly", "CL": "Colgate", "GIS": "GenMills", "HSY": "Hershey",
    "STZ": "Constell", "MNST": "Monster", "TJX": "TJX Cos", "ROST": "RossStores",
    "ULTA": "Ulta", "KHC": "KraftHeinz", "MDLZ": "Mondelez", "KDP": "Keurig",
    "DIS": "Disney", "BKNG": "Booking", "MAR": "Marriott", "HLT": "Hilton",
    "RCL": "RoyalCar", "CCL": "Carnival", "SPOT": "Spotify", "SHOP": "Shopify",
    "MELI": "MercadoL", "PARA": "Paramount", "WBD": "Warner", "FOXA": "Fox",
    "FOX": "Fox Corp", "MTCH": "Match", "Z": "Zillow", "DKNG": "DraftK",
    "RBLX": "Roblox", "U": "Unity", "TOST": "Toast", "MGM": "MGM", "NCLH": "Norwegian",
    "EXPE": "Expedia", "SIRI": "SiriusXM", "EA": "ElecArts", "XOM": "Exxon",
    "CVX": "Chevron", "COP": "Conoco", "SLB": "Schlumberger", "HAL": "Halliburton",
    "BKR": "Baker", "EOG": "EOG Res", "OXY": "Occidental", "MPC": "Marathon",
    "VLO": "Valero", "PSX": "Phillips66", "FANG": "Diamondb", "HES": "Hess",
    "DVN": "Devon", "GE": "GE Aero", "CAT": "Caterpillar", "MMM": "3M",
    "HON": "Honeywell", "DE": "Deere", "UNP": "UnionPac", "UPS": "UPS",
    "FDX": "FedEx", "EMR": "Emerson", "ETN": "Eaton", "ITW": "Illinois",
    "PH": "Parker", "PCAR": "PACCAR", "CMI": "Cummins", "ROK": "Rockwell",
    "TT": "Trane", "CARR": "Carrier", "GWW": "Grainger", "URI": "UnitedRent",
    "FAST": "Fastenal", "NUE": "Nucor", "FCX": "Freeport", "NEM": "Newmont",
    "DOW": "Dow", "WM": "WasteMgmt", "RSG": "Republic", "APD": "AirProd",
    "SHW": "Sherwin", "ECL": "Ecolab", "CPRT": "Copart", "SNA": "Snapon",
    "GPC": "GenParts", "EXPD": "Expeditors", "NEE": "NextEra", "DUK": "Duke",
    "SO": "Southern", "PLD": "Prologis", "AMT": "AmTower", "D": "Dominion",
    "EXC": "Exelon", "AEP": "AmElec", "SRE": "Sempra", "CEG": "ConstellEn",
    "BXP": "BostonProp", "CBRE": "CBRE", "CCI": "CrownCas", "SPG": "Simon",
    "O": "RealtyInc", "PSA": "PublicStor", "DLR": "DigitalRe", "EQIX": "Equinix",
    "VTR": "Ventas", "AVB": "Avalon", "ARE": "Alexandria", "SBAC": "SBACom",
    "WELL": "Welltower", "HST": "HostHot", "CSGP": "CoStar",
    "BME:SAN": "Santander", "BME:BBVA": "BBVA", "BME:CABK": "CaixaBank", "BME:BKT": "Bankinter",
    "BME:SAB": "Sabadell", "BME:UNI": "Unicaja", "BME:ITX": "Inditex", "BME:TEF": "Telefonica",
    "BME:IBE": "Iberdrola", "BME:REP": "Repsol", "BME:ELE": "Endesa", "BME:ENG": "Enagas",
    "BME:NTGY": "Naturgy", "BME:GRF": "Grifols", "BME:ROVI": "Rovi", "BME:PHM": "PharmaMar",
    "BME:ALM": "Almirall", "BME:AMS": "Amadeus", "BME:FER": "Ferrovial", "BME:ACS": "ACS",
    "BME:SCYR": "Sacyr", "BME:IAG": "IAG", "BME:AENA": "Aena", "BME:IDR": "Indra",
    "BME:MAP": "Mapfre", "BME:GCO": "CatOcc", "BME:COL": "Colonial", "BME:MER": "Merlin",
    "BME:SLR": "Solaria", "BME:LOG": "Logista", "BME:VIS": "Viscofan", "BME:MEL": "Melia",
    "BME:EBRO": "Ebro", "BME:FDR": "Fluidra", "BME:ACX": "Acerinox", "BME:ENC": "Ence",
    "MC.PA": "LVMH", "RMS.PA": "Hermes", "OR.PA": "LOreal", "TTE.PA": "Total",
    "SAP.DE": "SAP", "BMW.DE": "BMW", "MBG.DE": "Mercedes", "VOW3.DE": "VW",
    "BAS.DE": "Basf", "LIN": "Linde", "ENEL.MI": "Enel", "HSBA.L": "HSBC",
    "AZN.L": "AstraZ", "ADYEN.AS": "Adyen", "BABA": "Alibaba", "TCEHY": "Tencent",
    "JD": "JD.com", "PDD": "Pinduoduo", "BIDU": "Baidu", "NTES": "NetEase",
    "NIO": "NIO", "XPEV": "XPeng", "LI": "Li Auto", "BYDDF": "BYDZK",
    "GELYF": "Geely", "XIAOF": "Xiaomi", "MEIT": "Meituan", "KUAIF": "Kuaishou",
    "TME": "TencMusic", "FUTU": "Futu", "BEKE": "KE Hold", "TAL": "TAL Edu",
    "EDU": "NewOrient", "VIPS": "Vipshop", "GDS": "GDS", "JKS": "JinkoSolar",
    "DQ": "Daqo", "SMIC": "SMIC", "TSM": "TSMC", "SONY": "Sony", "TM": "Toyota",
    "HMC": "Honda", "9984.T": "SoftBank", "RELIANCE.NS": "Reliance", "HDFCBANK.NS": "HDFC",
    "INFY": "Infosys", "NU": "Nubank", "PBR": "Petrobras", "VALE": "Vale",
    "WALMEX.MX": "Walmex", "SE": "Sea Ltd", "GRAB": "Grab", "CPNG": "Coupang",
    "T": "AT&T", "VZ": "Verizon", "CMCSA": "Comcast", "ADSK": "Autodesk",
    "PAYX": "Paychex", "CTAS": "Cintas", "ORLY": "OReilly", "CSX": "CSX",
    "ADI": "AnalogDev", "KLAC": "KLAC", "PC": "CanPac", "CNI": "CanNat",
    "ADM": "Archer", "BBY": "BestBuy", "DHI": "DRHorton", "HAS": "Hasbro",
    "RL": "RalphLaur", "TPR": "Tapestry", "WBA": "Walgreens", "WYNN": "Wynn",
    "AAL": "AmAir", "LUV": "Southwest", "DAL": "DeltaAir", "UAL": "UnitedAir",
    "OKE": "Oneok", "JKHY": "JackHen", "POOL": "Pool", "FFIV": "F5",
    "JNPR": "Juniper", "MHK": "Mohawk", "NWL": "Newell", "LEN": "Lennar",
    "PHM": "Pulte", "NVR": "NVR", "TMUS": "TMobile", "CDW": "CDW Corp",
    "VRSK": "Verisk", "BR": "Broadridge", "PAYC": "Paycom", "GDDY": "GoDaddy",
    "GEN": "GenDig", "DT": "Dynatrace", "SPLK": "Splunk", "SWKS": "Skyworks",
    "QRVO": "Qorvo", "LSCC": "Lattice", "POWI": "PowerInt", "RMBS": "Rambus",
    "WDC": "WesternDig", "OLED": "UnivDisp", "MKSI": "MKSI", "MPWR": "Monolithic",
    "ENTG": "Entegris", "TER": "Teradyne", "ALTR": "Altair", "ACN": "Accenture",
    "ADP": "ADP", "DRI": "Darden", "IBM": "IBM", "CSCO": "Cisco", "QCOM": "Qualcomm",
    "INTC": "Intel", "MU": "Micron", "AMAT": "AppliedMat", "LRCX": "LamRes",
    "NXPI": "NXP Semi", "MCHP": "Microchip", "SNPS": "Synopsys", "CDNS": "Cadence",
    "WDAY": "Workday", "INTU": "Intuit", "VFC": "VF Corp", "K": "Kellanova",
    "CAG": "Conagra", "SJM": "Smucker", "CPB": "Campbell", "HRL": "Hormel",
    "TSN": "Tyson", "MKC": "McCormick", "CLX": "Clorox", "CHD": "ChurchDwight",
    "ELV": "Elevance", "COR": "Cencora", "MOH": "Molina", "HIG": "Hartford",
    "CINF": "Cincinnati", "L": "Loews", "RE": "Everest", "WRB": "Berkley",
    "CMA": "Comerica", "ZION": "Zions", "CFR": "Cullen", "WTFC": "Wintrust",
    "FHN": "FirstHor", "PBCT": "Pinnacle", "SNV": "Synovus", "WAL": "WesternAll",
    "FDS": "FactSet", "BRO": "Brown", "AJG": "Arthur", "GL": "Globe",
    "WLTW": "Willis", "CBOE": "CBOE", "NDAQ": "Nasdaq", "MSCI": "MSCI",
    "INFO": "IHS", "EPAM": "EPAM", "IT": "Gartner", "CTSH": "Cognizant",
    "PTC": "PTC", "TYL": "TylerTech", "FICO": "FairIsaac", "EFX": "Equifax",
    "TRMB": "Trimble", "VST": "Vistra", "TLN": "Talen Ene", "GFS": "GlobalFoundries",
    "ALAB": "Astera Labs", "CRSR": "Corsair", "YOU": "ClearSecure", "TENB": "Tenable",
    "CYBR": "CyberArk", "CHKP": "CheckPoint", "QLYS": "Qualys", "VRNS": "Varonis",
    "RPD": "Rapid7", "MNDY": "Monday", "NYSE:SMAR": "Smartsheet", "GTLB": "GitLab",
    "ASAN": "Asana", "CFLT": "Confluent", "HCP": "HashiCorp", "ESTC": "Elastic",
    "AYX": "Alteryx", "DOCN": "DigitalOcean", "FSLY": "Fastly", "PCOR": "Paycor",
    "PRO": "Procore", "NCNO": "nCino", "PCTY": "Paylocity", "DAY": "Dayforce",
    "HUBS": "HubSpot", "TWLO": "Twilio", "DBX": "Dropbox", "PD": "PagerDuty",
    "FIVN": "Five9", "BLNK": "Blink Charg", "CHPT": "ChargePoint", "RIVN": "Rivian",
    "LCID": "Lucid", "PSNY": "Polestar", "F": "Ford", "GM": "GenMotors",
    "APO": "Apollo", "KKR": "KKR & Co", "CG": "Carlyle", "ARES": "Ares Mgmt",
    "OAK": "Oaktree", "TROW": "TRowePrice", "STNE": "StoneCo", "PAGS": "PagSeguro",
    "DLO": "DLocal", "MKTX": "MarketAxess", "TW": "Tradeweb", "LPLA": "LPL Fin",
    "RJF": "Raymond", "JSF": "Stifel", "CORZ": "Core Sci", "BTDR": "Bitdeer",
    "SDIG": "Stronghold", "ANY": "Sphere 3D", "ARBK": "Argo Block", "WGMI": "TeraWulf",
    "OBDC": "TeraWulf", "BSM": "MidAmerica", "CPT": "Camden", "MAA": "MidAmerica",
    "ESS": "Essex Prop", "UDR": "UDR Inc", "IRM": "IronMount", "EXR": "ExtraSpace",
    "CUBE": "CubeSmart", "INVH": "Invitation", "EQR": "EquityRes", "KIM": "Kimco",
    "REG": "Regency", "NVO": "NovoNordisk", "NSRGY": "Nestle", "RHHBY": "Roche",
    "NVS": "Novartis", "SNY": "Sanofi", "SIEGY": "Siemens", "SUG": "Schneider",
    "ALV.DE": "Allianz", "AI.PA": "AirLiquide", "BN.PA": "Danone", "BME:RED": "RedElec",
    "BME:CIE": "CIE Auto", "BME:NHH": "NH Hoteles", "BME:TLGO": "Talgo", "PRX.AS": "Prosus",
    "ASTS": "AST Space", "RKLB": "RocketLab", "LUNR": "IntuitiveMac", "SPIR": "Spire",
    "PL": "PlanetLabs", "BKSY": "BlackSky", "RDW": "Redwire", "SATL": "Satellogic",
    "SIDU": "Sidus Space", "MNTS": "Momentus", "LLAP": "TerranOrb", "JOBY": "Joby Aviat",
    "ACHR": "Archer", "BLDE": "Blade Air", "EVE": "Eve Hold", "LILM": "Lilium",
    "VLD": "Velo3D", "TXN": "TexasInst", "ON": "ON Semi", "WOLF": "Wolfspeed",
    "XEL": "Xcel Energy", "ED": "ConEdison", "PEG": "PubServ", "WEC": "WEC Energy",
    "AWK": "AmWater", "ES": "Eversource", "WMB": "Williams", "KMI": "Kinder",
    "MTRG": "PTargaRes", "LNG": "Cheniere", "MUFG": "Mitsubishi", "SMFG": "Sumitomo",
    "MIELY": "Mitsui", "TAK": "Takeda", "RYAAY": "Ryanair", "ERIC": "Ericsson",
    "NOK": "Nokia", "STLA": "Stellantis", "RACE": "Ferrari", "MKL": "Markel",
    "CNA": "CNA Fin", "LNC": "Lincoln", "PFG": "PrinFin", "AIZ": "Arthur J",
    "WTW": "WillisTow", "ANSS": "Ansys", "J": "Jacobs", "BAH": "BoozAllen",
    "CACI": "CACI Intl", "SAIC": "SAIC", "MANT": "ManTech", "OSK": "Oshkosh",
    "FLS": "Fluor", "ATI": "ATI Inc", "MOG.A": "Moog", "KAMN": "Kaman",
    "CW": "Curtiss", "AJRD": "Aerojet", "AXON": "Axon", "VMI": "Valmont",
    "TNC": "Tennant", "LII": "Lennox", "AOS": "AO Smith", "PKI": "PerkinElmer",
    "BIO": "BioRad", "MTD": "Mettler", "TECH": "BioTechne", "CTLT": "Catalent"
}

opciones_desplegable = [f"{ticker} ({nombre})" for ticker, nombre in tickers_nombres.items()]
opciones_desplegable.sort()

# --- FUNCIONES AUXILIARES GLOBALES ---
def obtener_simbolo_moneda(ticker):
    ticker_upper = ticker.upper()
    if ticker_upper.startswith("BME:") or ticker_upper.endswith((".MC", ".DE", ".PA", ".MI", ".AS")):
        return "€"
    elif ticker_upper.endswith(".T"):
        return "¥"
    elif ticker_upper.endswith(".L"):
        return "GBp"
    elif ticker_upper.endswith(".ST"):
        return "kr"
    elif ticker_upper.endswith(".NS"):
        return "₹"
    else:
        return "$"

def obtener_region(ticker):
    if "BME:" in ticker or ticker.endswith((".DE", ".PA", ".MI", ".L", ".AS", ".ST")): return "Europa"
    elif ticker.endswith((".T", ".NS")) or ticker in ["BABA", "TCEHY", "JD", "PDD", "BIDU", "NTES", "NIO", "XPEV", "LI", "BYDDF", "GELYF", "XIAOF", "MEIT", "KUAIF", "TME", "FUTU", "BEKE", "TAL", "EDU", "VIPS", "GDS", "JKS", "DQ", "SMIC", "TSM", "SONY", "TM", "HMC", "SE", "GRAB", "CPNG"]: return "Asia"
    else: return "EEUU"

def a_yahoo(ticker):
    if ticker.startswith("BME:"): return ticker.replace("BME:", "") + ".MC"
    if ticker.startswith("NYSE:"): return ticker.replace("NYSE:", "")
    return ticker

def obtener_estado_mercados():
    ahora_utc = datetime.datetime.now(pytz.utc)
    hora_madrid = ahora_utc.astimezone(pytz.timezone('Europe/Madrid'))
    t_madrid = hora_madrid.time()
    
    horario_us = "15:30 - 22:00"
    horario_eu = "09:00 - 17:30"
    horario_as = "02:00 - 09:00"

    if hora_madrid.weekday() >= 5: 
        est_us, est_eu, est_as = "🔴 Cerrado", "🔴 Cerrado", "🔴 Cerrado"
    else:
        if datetime.time(10, 0) <= t_madrid < datetime.time(15, 30): est_us = "🟡 Pre-Market"
        elif datetime.time(15, 30) <= t_madrid < datetime.time(22, 0): est_us = "🟢 Abierto"
        elif datetime.time(22, 0) <= t_madrid <= datetime.time(23, 59): est_us = "🔵 Post-Market"
        else: est_us = "🔴 Cerrado"

        if datetime.time(9, 0) <= t_madrid < datetime.time(17, 30): est_eu = "🟢 Abierto"
        else: est_eu = "🔴 Cerrado"

        if datetime.time(2, 0) <= t_madrid < datetime.time(9, 0): est_as = "🟢 Abierto"
        else: est_as = "🔴 Cerrado"
    
    return {"estado": est_us, "horario": horario_us}, {"estado": est_eu, "horario": horario_eu}, {"estado": est_as, "horario": horario_as}

# ---> FUNCIÓN DE COLOR MOVIDA A ZONA GLOBAL <---
def color_pct(val):
    if isinstance(val, str) and '%' in val:
        if val.startswith('+'): return 'color: #228B22;' 
        elif val.startswith('-'): return 'color: #FF3333;' 
    return ''

# ==========================================
# 4. CABECERA Y SEMÁFOROS
# ==========================================
us, eu, asia = obtener_estado_mercados()
col1, col2, col3 = st.columns(3)
col1.info(f"**EE.UU:** {us['estado']} | Hora: {us['horario']} (Madrid)")
col2.info(f"**Europa:** {eu['estado']} | Hora: {eu['horario']} (Madrid)")
col3.info(f"**Asia:** {asia['estado']} | Hora: {asia['horario']} (Madrid)")
st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs(["🔬 Análisis Individual", "⚔️ Análisis Colectivo", "🎯 Cazar Alpha (Radar)", "🏆 Sala de Trofeos"])

# ------------------------------------------
# PESTAÑA 1: VISOR DE GRÁFICOS (VERSIÓN DEFINITIVA Y ESTABLE)
# ------------------------------------------
with tab1:
    st.markdown("### 🔍 Selector de Activos")
    
    col_buscador, col_logo, col_espacio = st.columns([0.8, 0.4, 2.8])
    with col_buscador:
        ticker_elegido = st.selectbox("Elige la empresa que quieres revisar:", opciones_desplegable)
    with col_logo:
        espacio_logo = st.empty() 
        
    espacio_descripcion = st.empty() 
    espacio_sector = st.empty()
    
    if ticker_elegido:
        simbolo_real = ticker_elegido.split(" ")[0]
        simbolo_yahoo = a_yahoo(simbolo_real)
        st.markdown("---")
        
        contenedor_cajas = st.container()
        
        st.markdown("<br>", unsafe_allow_html=True) 
        
        col_tiempo, col_tendencias = st.columns([3, 1.5])
        with col_tiempo:
            periodo = st.radio("⏱️ Rango de tiempo:", ["1 Mes", "3 Meses", "6 Meses", "1 Año", "5 Años", "10 Años", "Máximo"], index=1, horizontal=True)
            
        with col_tendencias:
            mostrar_tendencia = st.toggle("📈 SMA 50 (Medio)", help="Media Móvil de 50 días. Mide el pulso a medio plazo.")
            mostrar_tendencia_200 = st.toggle("🚀 SMA 200 (Largo)", help="Media de 200 días. Si la SMA 50 cruza por encima de esta línea, se produce un 'Cruce de Oro' (Señal alcista muy fuerte).")
            
        with st.spinner(f"Cargando datos de {simbolo_real} y rastreando Wall Street..."):
            try:
                datos_brutos = yf.download(simbolo_yahoo, period="max", progress=False)
                if isinstance(datos_brutos.columns, pd.MultiIndex): datos_brutos.columns = datos_brutos.columns.get_level_values(0)

                if not datos_brutos.empty and 'Close' in datos_brutos.columns:
                    
                    recom, precio_obj_str, fecha_earnings, sector, insider_trend = "Sin noticias", "Sin noticias", "Sin noticias", "Sin noticias", "Sin noticias"
                    desc_corta = ""
                    logo_url = ""
                    
                    API_FINNHUB = "d7c2s5hr01quh9fcasf0d7c2s5hr01quh9fcasfg"
                    
                    info = {}
                    ticker_obj = None
                    for _ in range(3):
                        try:
                            ticker_obj = yf.Ticker(simbolo_yahoo)
                            temp_info = ticker_obj.info
                            if isinstance(temp_info, dict) and len(temp_info) > 5:
                                info = temp_info
                                break  
                        except: pass
                        time.sleep(0.5) 
                    
                    try:
                        if info:
                            sector = info.get('sector', 'Sin noticias')
                            descripcion_completa = info.get('longBusinessSummary', 'Sin descripción disponible.')
                            recom_raw = info.get('recommendationKey')
                            p_obj = info.get('targetMeanPrice')
                            
                            if descripcion_completa != 'Sin descripción disponible.' and descripcion_completa:
                                fragmentos = descripcion_completa.split('. ')
                                desc_corta = '. '.join(fragmentos[:2]) + '.' if len(fragmentos) > 1 else descripcion_completa
                                if len(desc_corta) > 300: desc_corta = desc_corta[:297] + "..."
                            
                            if recom_raw:
                                traducciones = {"strong_buy": "COMPRA FUERTE 🟢", "buy": "COMPRAR ↗️", "hold": "MANTENER 🟡", "sell": "VENTA ↘️", "strong_sell": "VENTA MASIVA 🔴"}
                                recom = traducciones.get(recom_raw.lower(), str(recom_raw).replace('_', ' ').upper())
                            
                            if p_obj and p_obj > 0: precio_obj_str = str(p_obj)
                            
                        try:
                            if ticker_obj:
                                cal = ticker_obj.calendar
                                if isinstance(cal, dict) and 'Earnings Date' in cal:
                                    fechas = cal['Earnings Date']
                                    if isinstance(fechas, list) and len(fechas) > 0 and pd.notnull(fechas[0]):
                                        fecha_earnings = fechas[0].strftime("%d/%m/%Y")
                        except: pass
                    except: pass
                        
                    try:
                        hoy = datetime.datetime.today().strftime('%Y-%m-%d')
                        pasado = (datetime.datetime.today() - datetime.timedelta(days=180)).strftime('%Y-%m-%d')
                        sym_finnhub = simbolo_yahoo if "." in simbolo_yahoo else simbolo_real
                        
                        try:
                            r_prof = requests.get(f"https://finnhub.io/api/v1/stock/profile2?symbol={sym_finnhub}&token={API_FINNHUB}", timeout=3).json()
                            if isinstance(r_prof, dict) and r_prof.get('logo'):
                                url_temp = r_prof['logo']
                                if url_temp.startswith("http"): logo_url = url_temp
                        except: pass
                        
                        if recom == "Sin noticias":
                            try:
                                r_rec = requests.get(f"https://finnhub.io/api/v1/stock/recommendation?symbol={sym_finnhub}&token={API_FINNHUB}", timeout=3).json()
                                if isinstance(r_rec, list) and len(r_rec) > 0:
                                    latest = r_rec[0]
                                    votos = {"COMPRA FUERTE 🟢": latest.get('strongBuy', 0), "COMPRAR ↗️": latest.get('buy', 0), "MANTENER 🟡": latest.get('hold', 0), "VENTA ↘️": latest.get('sell', 0), "VENTA MASIVA 🔴": latest.get('strongSell', 0)}
                                    ganador = max(votos, key=votos.get)
                                    if votos[ganador] > 0: recom = ganador
                            except: pass
                            
                        if precio_obj_str == "Sin noticias":
                            try:
                                r_pt = requests.get(f"https://finnhub.io/api/v1/stock/price-target?symbol={sym_finnhub}&token={API_FINNHUB}", timeout=3).json()
                                if isinstance(r_pt, dict) and r_pt.get('targetMean') and float(r_pt['targetMean']) > 0:
                                    precio_obj_str = str(r_pt['targetMean'])
                            except: pass

                        try:
                            r_ins = requests.get(f"https://finnhub.io/api/v1/stock/insider-sentiment?symbol={sym_finnhub}&from={pasado}&to={hoy}&token={API_FINNHUB}").json()
                            if isinstance(r_ins, dict) and 'data' in r_ins and len(r_ins['data']) > 0:
                                mspr = r_ins['data'][-1].get('mspr', 0)
                                if mspr > 5: insider_trend = "COMPRA MASIVA 🟢"
                                elif mspr > 0: insider_trend = "COMPRANDO ↗️"
                                elif mspr < -5: insider_trend = "VENTA MASIVA 🔴"
                                elif mspr < 0: insider_trend = "VENDIENDO ↘️"
                                else: insider_trend = "NEUTRAL ⚪"
                        except: pass
                            
                        if fecha_earnings == "Sin noticias":
                            try:
                                futuro = (datetime.datetime.today() + datetime.timedelta(days=90)).strftime('%Y-%m-%d')
                                r_earn = requests.get(f"https://finnhub.io/api/v1/calendar/earnings?from={hoy}&to={futuro}&symbol={sym_finnhub}&token={API_FINNHUB}").json()
                                if isinstance(r_earn, dict) and 'earningsCalendar' in r_earn and len(r_earn['earningsCalendar']) > 0:
                                    fecha_raw = r_earn['earningsCalendar'][0].get('date', 'Sin noticias')
                                    if fecha_raw != 'Sin noticias':
                                        fecha_earnings = datetime.datetime.strptime(fecha_raw, "%Y-%m-%d").strftime("%d/%m/%Y")
                            except: pass
                    except: pass 

                    if desc_corta:
                        espacio_descripcion.markdown(f"<div style='font-size: 14px; color: #666; margin-top: 5px; margin-bottom: 5px;'><i>{desc_corta}</i></div>", unsafe_allow_html=True)
                    
                    if sector != "Sin noticias":
                        espacio_sector.markdown(f"<div style='font-size: 14px; margin-bottom: 10px;'>🏢 <b>Sector:</b> {sector}</div>", unsafe_allow_html=True)
                        
                    if logo_url:
                        espacio_logo.markdown(f"<img src='{logo_url}' style='height: 70px; max-width: 140px; object-fit: contain; margin-top: 10px;'/>", unsafe_allow_html=True)
                    else:
                        espacio_logo.empty()

                    datos_limpios_completos = datos_brutos.dropna(subset=['Close'])
                    precio_actual = float(datos_limpios_completos['Close'].iloc[-1])
                    
                    sma_50_completa = datos_limpios_completos['Close'].rolling(window=50).mean()
                    sma_200_completa = datos_limpios_completos['Close'].rolling(window=200).mean()
                    
                    dias_mapa = {"1 Mes": 21, "3 Meses": 63, "6 Meses": 126, "1 Año": 252, "5 Años": 1260, "10 Años": 2520, "Máximo": len(datos_limpios_completos)}
                    dias_mostrar = dias_mapa.get(periodo, len(datos_limpios_completos))
                    
                    datos_limpios = datos_limpios_completos.iloc[-dias_mostrar:]
                    sma_50 = sma_50_completa.iloc[-dias_mostrar:]
                    sma_200 = sma_200_completa.iloc[-dias_mostrar:]
                    
                    s_moneda_visual = obtener_simbolo_moneda(simbolo_real)
                    
                    if precio_obj_str != "Sin noticias":
                        p_obj_f = float(precio_obj_str)
                        pot = ((p_obj_f / precio_actual) - 1) * 100
                        color_p = "#228B22" if pot > 0 else "#FF3333"
                        precio_obj_final = f"{p_obj_f:,.2f} {s_moneda_visual} <span style='color:{color_p};font-weight:bold;font-size:13px;'>({pot:+.1f}%)</span>"
                    else: precio_obj_final = "Sin noticias"
                        
                    precio_usd = None
                    mapa_divisas = { "€": "EURUSD=X", "¥": "JPYUSD=X", "GBp": "GBPUSD=X", "kr": "SEKUSD=X", "₹": "INRUSD=X" }
                    t_div = mapa_divisas.get(s_moneda_visual)
                    if t_div:
                        try:
                            d_div = yf.download(t_div, period="5d", progress=False)
                            if not d_div.empty:
                                if isinstance(d_div.columns, pd.MultiIndex): d_div.columns = d_div.columns.get_level_values(0)
                                p_div = d_div['Close']
                                if isinstance(p_div, pd.DataFrame): p_div = p_div.iloc[:, 0]
                                tasa = float(p_div.dropna().iloc[-1])
                                precio_usd = (precio_actual * tasa) / 100 if s_moneda_visual == "GBp" else (precio_actual * tasa)
                        except: pass

                    t_conv = f'<span style="font-size:18px;color:#7f8c8d;font-weight:400;margin-left:10px;">(≈ {precio_usd:,.2f} $)</span>' if precio_usd else ""
                    
                    with contenedor_cajas:
                        st.markdown(f"""
                        <div style="background-color:#f8f9fa;padding:15px;border-radius:10px;box-shadow:0 4px 6px rgba(0,0,0,0.05);margin-bottom:20px;">
                            <div style="display:flex;justify-content:space-between;align-items:center;">
                                <div>
                                    <p style="margin:0;font-size:14px;color:rgba(49,51,63,0.7);">Valor Actual ({simbolo_real})</p>
                                    <h2 style="margin:0;font-weight:700;color:#1f1f1f;font-size:32px;">{precio_actual:,.2f} {s_moneda_visual}{t_conv}</h2>
                                </div>
                            </div>
                        </div>
                        <div style="display:flex;gap:15px;margin-bottom:20px;">
                            <div title="Consenso de analistas de inversión y precio objetivo promedio a 12 meses." style="flex:1;background:#fff;padding:15px;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.05);border-top:4px solid #1E90FF;cursor:help;">
                                <div style="font-size:12px;color:#7f8c8d;text-transform:uppercase;font-weight:bold;margin-bottom:5px;">🏦 Wall Street ℹ️</div>
                                <div style="font-size:14px;color:#2c3e50;"><b>Consenso:</b> {recom}</div>
                                <div style="font-size:14px;color:#2c3e50;margin-top:5px;"><b>Precio Obj:</b> {precio_obj_final}</div>
                            </div>
                            <div title="Próxima fecha confirmada o estimada de resultados financieros trimestrales." style="flex:1;background:#fff;padding:15px;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.05);border-top:4px solid #f39c12;cursor:help;">
                                <div style="font-size:12px;color:#7f8c8d;text-transform:uppercase;font-weight:bold;margin-bottom:5px;">📅 Próximos Earnings ℹ️</div>
                                <div style="font-size:18px;color:#2c3e50;font-weight:bold;margin-top:5px;">{fecha_earnings}</div>
                            </div>
                            <div title="Muestra si los directivos (CEO, dueños) han estado comprando o vendiendo acciones propias recientemente." style="flex:1;background:#fff;padding:15px;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.05);border-top:4px solid #8e44ad;cursor:help;">
                                <div style="font-size:12px;color:#7f8c8d;text-transform:uppercase;font-weight:bold;margin-bottom:5px;">👔 Manos Fuertes ℹ️</div>
                                <div style="font-size:14px;color:#2c3e50;"><b>Directivos (6M):</b> {insider_trend}</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                    # --- GRÁFICA CON DOS TENDENCIAS ---
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=datos_limpios.index, y=datos_limpios['Close'], mode='lines', name='Precio', line=dict(color='#228B22', width=2)))
                    
                    if mostrar_tendencia:
                        if not sma_50.isna().all():
                            fig.add_trace(go.Scatter(
                                x=datos_limpios.index, 
                                y=sma_50, 
                                mode='lines', 
                                name='SMA 50 (Medio)', 
                                line=dict(color='#FFA500', width=2.5) 
                            ))
                            
                    if mostrar_tendencia_200:
                        if not sma_200.isna().all():
                            fig.add_trace(go.Scatter(
                                x=datos_limpios.index, 
                                y=sma_200, 
                                mode='lines', 
                                name='SMA 200 (Largo)', 
                                line=dict(color='#9b59b6', width=2.5, dash='dot')
                            ))
                        else:
                            st.info("La empresa es muy reciente y no tiene 200 días de historia para calcular la tendencia a largo plazo.")

                    fig.update_layout(
                        title=f"Histórico: {ticker_elegido}", 
                        template='plotly_dark', 
                        margin=dict(l=0, r=0, t=40, b=0), 
                        hovermode="x unified",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                else: st.warning("⚠️ Sin datos disponibles.")
            except Exception as e: st.error(f"⚠️ Error técnico: {e}")
# ------------------------------------------
# PESTAÑA 2: BATALLA DE ALPHA (COMPARATIVA)
# ------------------------------------------
with tab2:
    st.markdown("### ⚔️ Comparativa Múltiple")
    st.write("Selecciona varios activos para ver cuál está rindiendo mejor en un mismo periodo de tiempo. Todos empezarán en Base 0 (0% de rendimiento) para una comparación justa.")
    
    col_comp1, col_comp2 = st.columns([3, 1])
    
    with col_comp1:
        opciones_comp = ["SPY (S&P 500)", "QQQ (Nasdaq 100)"] + opciones_desplegable
        seleccionados = st.multiselect(
            "Elige los activos a enfrentar (Puedes elegir todos los que quieras):", 
            opciones_comp, 
            default=["SPY (S&P 500)"]
        )
        
    with col_comp2:
        periodo_comp = st.radio(
            "Rango de tiempo:", 
            ["1 Mes", "3 Meses", "6 Meses", "1 Año", "5 Años", "Máximo"], 
            index=3,
            key="radio_batalla" 
        )
        
    mapa_tiempo_comp = {"1 Mes": "1mo", "3 Meses": "3mo", "6 Meses": "6mo", "1 Año": "1y", "5 Años": "5y", "Máximo": "max"}

    if seleccionados:
        if st.button("🚀 Iniciar Batalla de Rendimiento", use_container_width=True):
            with st.spinner("Descargando históricos y sincronizando la escala a Base 100..."):
                fig_comp = go.Figure()
                
                for sel in seleccionados:
                    sym_real = sel.split(" ")[0]
                    sym_y = "SPY" if sym_real == "SPY" else ("QQQ" if sym_real == "QQQ" else a_yahoo(sym_real))
                    
                    try:
                        df_comp = yf.download(sym_y, period=mapa_tiempo_comp[periodo_comp], progress=False)
                        if isinstance(df_comp.columns, pd.MultiIndex): 
                            df_comp.columns = df_comp.columns.get_level_values(0)
                        
                        df_comp = df_comp.dropna(subset=['Close'])
                        
                        if not df_comp.empty:
                            cierres_comp = df_comp['Close'].squeeze()
                            precio_inicial = float(cierres_comp.iloc[0])
                            pct_cambio = ((cierres_comp / precio_inicial) - 1) * 100
                            
                            es_indice = sym_real in ["SPY", "QQQ"]
                            grosor = 3 if es_indice else 2
                            estilo_linea = 'dot' if es_indice else 'solid'
                            
                            fig_comp.add_trace(go.Scatter(
                                x=pct_cambio.index, 
                                y=pct_cambio, 
                                mode='lines', 
                                name=sym_real,
                                line=dict(width=grosor, dash=estilo_linea)
                            ))
                    except Exception as e:
                        st.warning(f"⚠️ No se pudo cargar el histórico de {sym_real}.")

                fig_comp.update_layout(
                    title=f"Rendimiento Comparativo Acumulado",
                    template='plotly_dark',
                    xaxis_title="",
                    yaxis_title="Rendimiento Acumulado (%)",
                    hovermode="x unified",
                    margin=dict(l=0, r=0, t=40, b=0)
                )
                
                st.plotly_chart(fig_comp, use_container_width=True)

# ------------------------------------------
# PESTAÑA 3: RADAR DE CAZA (MOTOR "OPPENHEIMER" V2.0 - ANTI-BORRADO)
# ------------------------------------------
with tab3:
    st.markdown("### 🎯 Centro de Caza Oppenheimer")
    
    c1, c2, c3, c4 = st.columns(4)
    btn_todos = c1.button("Cazar Todos los Mercados", use_container_width=True)
    btn_us = c2.button("Cazar Solo EE.UU.", use_container_width=True)
    btn_eu = c3.button("Cazar Solo Europa", use_container_width=True)
    btn_asia = c4.button("Cazar Solo Asia", use_container_width=True)
    
    st.markdown("---")

    mercado_objetivo = None
    if btn_todos: mercado_objetivo = "Todos"
    elif btn_us: mercado_objetivo = "EEUU"
    elif btn_eu: mercado_objetivo = "Europa"
    elif btn_asia: mercado_objetivo = "Asia"

    if mercado_objetivo:
        st.session_state.resultados_radar = None
        # Semáforos de mercado
        if mercado_objetivo == "EEUU" and "Cerrado" in us["estado"]:
            st.warning("⚠️ **Aviso:** Wall Street está cerrado. Los datos son del último cierre.")
        elif mercado_objetivo == "Europa" and "Cerrado" in eu["estado"]:
            st.warning("⚠️ **Aviso:** Mercado europeo cerrado.")
        elif mercado_objetivo == "Asia" and "Cerrado" in asia["estado"]:
            st.warning("⚠️ **Aviso:** Mercado asiático cerrado.")

        tickers_a_escanear = [t for t in tickers_nombres.keys() if mercado_objetivo == "Todos" or obtener_region(t) == mercado_objetivo]
        
        # Creamos un contenedor para poder cambiar el texto luego
        mensaje_estado = st.empty()
        mensaje_estado.info(f"🚀 Ejecutando Algoritmo Oppenheimer ULTRA para: **{mercado_objetivo}**...")
        
        barra_progreso = st.progress(0, text="Calibrando benchmark global y escaneando ADN de activos...")
        resultados_temporales = []
        
        # 1. CALIBRACIÓN BENCHMARK
        alphaSPY_1m = 0
        try:
            spy_data = yf.download("SPY", period="1y", progress=False)
            if isinstance(spy_data.columns, pd.MultiIndex): spy_data.columns = spy_data.columns.get_level_values(0)
            spy_cierres = spy_data['Close'].dropna()
            alphaSPY_1m = ((float(spy_cierres.iloc[-1]) / float(spy_cierres.iloc[-21])) - 1) * 100
        except Exception: pass

        ws = conectar_db()
        existentes_en_db = []
        if ws:
            try: existentes_en_db = ws.col_values(1)
            except: pass

# --- 1.5 PRE-CARGADOR DE DIVISAS (Súper rápido y seguro) ---
        fx_rates = {}
        mapa_divisas_fx = { "€": "EURUSD=X", "¥": "JPYUSD=X", "GBp": "GBPUSD=X", "kr": "SEKUSD=X", "₹": "INRUSD=X" }
        for mon_sim, par_fx in mapa_divisas_fx.items():
            try:
                d_fx = yf.download(par_fx, period="1d", progress=False)
                if not d_fx.empty:
                    if isinstance(d_fx.columns, pd.MultiIndex): d_fx.columns = d_fx.columns.get_level_values(0)
                    fx_rates[mon_sim] = float(d_fx['Close'].dropna().iloc[-1])
            except: pass
        
        # 2. BUCLE CUANTITATIVO
        for i, ticker in enumerate(tickers_a_escanear):
            porcentaje = int(((i + 1) / len(tickers_a_escanear)) * 100)
            barra_progreso.progress((i + 1) / len(tickers_a_escanear), text=f"⏳ `Evaluando: {ticker.ljust(6)} | {porcentaje}%`")
            
            try:
                sym_y = a_yahoo(ticker)
                data = yf.download(sym_y, period="5y", progress=False)
                if isinstance(data.columns, pd.MultiIndex): data.columns = data.columns.get_level_values(0)
                df = data[['Close', 'High', 'Low', 'Volume']].dropna()
                if len(df) < 55: continue 
                
                # --- HACK INTRADÍA: FORZAR EL PRECIO DE AHORA MISMO ---
                try:
                    data_ahora = yf.download(sym_y, period="1d", interval="1m", progress=False)
                    if not data_ahora.empty:
                        if isinstance(data_ahora.columns, pd.MultiIndex): data_ahora.columns = data_ahora.columns.get_level_values(0)
                        # Sobrescribimos el último precio diario con el último precio de este minuto
                        df.iloc[-1, df.columns.get_loc('Close')] = float(data_ahora['Close'].iloc[-1])
                        df.iloc[-1, df.columns.get_loc('Volume')] = float(data_ahora['Volume'].sum()) # Sumamos el volumen del día
                        df.iloc[-1, df.columns.get_loc('High')] = float(max(df.iloc[-1]['High'], data_ahora['High'].max()))
                except: pass
                # -----------------------------------------------------

                c_hoy = float(df['Close'].iloc[-1])
                c_ayer = float(df['Close'].iloc[-2])
                pct_h = ((c_hoy / c_ayer) - 1) * 100
                vol_h = float(df['Volume'].iloc[-1])
                vol_m = float(df['Volume'].iloc[-20:].mean())
                sma50_serie = df['Close'].rolling(window=50).mean()
                sma50 = float(sma50_serie.iloc[-1])
                sma50_prev = float(sma50_serie.iloc[-6])
                sma200 = float(df['Close'].rolling(window=200).mean().iloc[-1]) if len(df) >= 200 else None
                delta = df['Close'].diff()
                gain = delta.where(delta > 0, 0).rolling(window=14).mean()
                loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
                rsi = float(100 - (100 / (1 + (gain / loss))).iloc[-1])
                def ret_d(d): return ((c_hoy / float(df['Close'].iloc[-(d+1)])) - 1) * 100 if len(df) > d else 0
                r1m, r6m, r1y, r5y = ret_d(21), ret_d(126), ret_d(252), ret_d(1260)
                max_52 = float(df['High'].iloc[-252:].max())
                dist_max = ((c_hoy / max_52) - 1) * 100
                
                # OBV y ATR
                obv = (np.sign(delta) * df['Volume']).fillna(0).cumsum()
                obv_hoy, obv_mes = float(obv.iloc[-1]), float(obv.iloc[-21])
                tr = pd.concat([df['High']-df['Low'], np.abs(df['High']-df['Close'].shift()), np.abs(df['Low']-df['Close'].shift())], axis=1).max(axis=1)
                atr = float(tr.rolling(14).mean().iloc[-1])
                stop_l = c_hoy - (atr * 2.5)
                
                p = 0
                status_t = "Lateral/Bajista"
                texto_a = []
                if c_hoy > sma50: p += 10; status_t = "Alcista Corto"
                if sma200 and c_hoy > sma200: p += 10
                if sma200 and sma50 > sma200: p += 10; status_t = "Alcista Estructural"
                if sma50 > sma50_prev: p += 10 
                if 55 <= rsi <= 68: p += 20; texto_a.append("RSI óptimo.")
                elif rsi > 72: p -= 15; texto_a.append("Riesgo sobrecompra.")
                if vol_h > (vol_m * 1.8) and pct_h > 0: p += 20; texto_a.append("Smart Money.")
                if obv_hoy > obv_mes: p += 10
                else: p -= 20; texto_a.append("⚠️ Divergencia OBV.")
                
                reg = obtener_region(ticker)
                b_1m = alphaSPY_1m if reg == "EEUU" else 0
                if r1m > (b_1m + 2.0): p += 10; texto_a.append("Bate al mercado.")
                
                isF = False
                if dist_max <= -20 and c_hoy > sma50 and vol_h > (vol_m * 1.8) and pct_h > 1.5:
                    p = max(p, 92); isF = True; status_t = "Giro Fénix 🔥"; texto_a = ["Giro explosivo."]
                
                pts = max(0, min(100, int(p)))
                if pts >= 90 and ticker not in existentes_en_db and ws is not None:
                    ws.append_row([ticker, tickers_nombres[ticker], datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), float(c_hoy), int(pts)])
                    existentes_en_db.append(ticker)

                reco = "⚪ ESPERAR"
                if pts >= 90: reco = "💎 COMPRA FUERTE (ALFA)" if not isF else "🔥 COMPRA (FÉNIX)"
                elif pts >= 80: reco = "🟢 ACUMULAR"
                elif pts >= 70: reco = "🟡 VIGILAR"

                # --- FORMATO FINAL CON MONEDA Y CONVERSIÓN ---
                mon = obtener_simbolo_moneda(ticker)
                
                # Calculamos la conversión a dólares usando la memoria del pre-cargador
                t_conv_stop = ""
                tasa_v = fx_rates.get(mon)
                if mon != "$" and tasa_v:
                    # Ajuste para la Libra (que cotiza en peniques)
                    factor = tasa_v / 100 if mon == "GBp" else tasa_v
                    t_conv_stop = f" (≈ {(stop_l * factor):.2f} $)"

                # Construimos los textos finales
                str_precio = f"{c_hoy:.2f} {mon}"
                str_stop = f"{stop_l:.2f} {mon}{t_conv_stop} ({((stop_l/c_hoy)-1)*100:+.1f}%)"

                resultados_temporales.append({
                    "TICKER": ticker, 
                    "NOMBRE": tickers_nombres[ticker], 
                    "PUNTOS": pts, 
                    "RECOMENDACIÓN": reco,
                    "TENDENCIA": status_t, 
                    "PRECIO HOY": str_precio,
                    "STOP LOSS": str_stop,
                    "% HOY": f"{pct_h:+.2f}%", 
                    "% 1 MES": f"{r1m:+.2f}%", 
                    "% 6 MESES": f"{r6m:+.2f}%", 
                    "% 1 AÑO": f"{r1y:+.2f}%", 
                    "% 5 AÑOS": f"{r5y:+.2f}%", 
                    "ANÁLISIS": " ".join(texto_a) if texto_a else "Estable."
                })
                time.sleep(0.01)
            except: continue
            
        barra_progreso.progress(100, text="✅ Caza Finalizada")
        st.session_state.resultados_radar = resultados_temporales
        st.session_state.mercado_cazado = mercado_objetivo # Memorizamos qué hemos cazado

    # --- ZONA DE DIBUJADO DE RESULTADOS ---
    if st.session_state.resultados_radar is not None:
        # Ponemos el mensaje verde de éxito AQUÍ para que solo salga al terminar
        merc_txt = st.session_state.get('mercado_cazado', 'Todos')
        st.success(f"🎯 Caza terminada para: **{merc_txt}**")
        
        if len(st.session_state.resultados_radar) == 0:
            st.info("🕵️‍♂️ **Escaneo completado:** Ningún activo de este mercado ha alcanzado hoy la excelencia (90+ puntos).")
        else:
            df_res = pd.DataFrame(st.session_state.resultados_radar)
            df_res = df_res.sort_values(by="PUNTOS", ascending=False).reset_index(drop=True)
            st.dataframe(df_res.style.map(color_pct, subset=["% HOY", "% 1 MES", "% 6 MESES", "% 1 AÑO", "% 5 AÑOS"]), 
                         use_container_width=True, hide_index=True,
                         column_config={"PUNTOS": st.column_config.NumberColumn(help="Nota 0-100."),
                                        "RECOMENDACIÓN": st.column_config.TextColumn(help="💎 ALFA: Cohete estable.\n🔥 FÉNIX: Rebote suelo."),
                                        "STOP LOSS": st.column_config.TextColumn(help="Precio de salida ATR en tu broker."),
                                        "ANÁLISIS": st.column_config.TextColumn(width="large")})

# ------------------------------------------
# PESTAÑA 4: SALA DE TROFEOS (PERSISTENTE)
# ------------------------------------------
with tab4:
    st.markdown("### 🏆 Sala de Trofeos")
    st.write("Auditoría persistente. Los datos se mantienen hasta que decidas re-auditar.")
    
    ws = conectar_db()
    if ws is not None:
        data_sheet = ws.get_all_records()
        if not data_sheet:
            st.info("La vitrina está vacía.")
        else:
            with st.expander("🗑️ Gestionar Base de Datos"):
                with st.form("del_form"):
                    tk_del = st.selectbox("Ticker a eliminar:", [d['Ticker'] for d in data_sheet])
                    if st.form_submit_button("Borrar"):
                        cell = ws.find(tk_del, in_column=1)
                        if cell: ws.delete_rows(cell.row); st.success(f"{tk_del} eliminado."); time.sleep(1); st.rerun()

            if st.button("🔄 Lanzar Auditoría de Rendimiento", use_container_width=True):
                with st.spinner("Sincronizando con Wall Street..."):
                    res_aud = {"exitos": [], "cuarentena": [], "fracasos": [], "pendiente": [], "w_rate": 0, "cohetes": 0}
                    ahora = datetime.datetime.now()
                    for d in data_sheet:
                        try:
                            ticker = d['Ticker']; tk_y = a_yahoo(ticker); mon = obtener_simbolo_moneda(ticker)
                            hist = yf.download(tk_y, period="5d", progress=False)
                            if isinstance(hist.columns, pd.MultiIndex): hist.columns = hist.columns.get_level_values(0)
                            p_hoy = float(hist['Close'].iloc[-1]); p_in = float(str(d['Precio_Aviso']).replace(',', '.'))
                            rent = ((p_hoy / p_in) - 1) * 100; f_ent = datetime.datetime.strptime(d['Fecha'], "%Y-%m-%d %H:%M")
                            dias = (ahora - f_ent).days
                            kpi = f"🚀 +5% en {dias}d" if rent >= 5.0 else (f"⏳ {dias}d" if rent > 0 else "")
                            obj = {"T": ticker, "N": d['Empresa'], "E": p_in, "A": p_hoy, "R": rent, "F": d['Fecha'], "KPI": kpi, "M": mon}
                            if abs(rent) < 0.1: res_aud["pendiente"].append(obj)
                            elif rent > 0: res_aud["exitos"].append(obj)
                            elif rent >= -3.0: res_aud["cuarentena"].append(obj)
                            else: res_aud["fracasos"].append(obj)
                        except: continue
                    tot_v = len(res_aud["exitos"]) + len(res_aud["cuarentena"]) + len(res_aud["fracasos"])
                    res_aud["w_rate"] = (len(res_aud["exitos"])/tot_v*100 if tot_v > 0 else 0)
                    res_aud["cohetes"] = len([x for x in res_aud["exitos"] if "🚀" in x['KPI']])
                    st.session_state.resultados_auditoria = res_aud

            # MOSTRAR RESULTADOS DESDE LA MEMORIA
            if st.session_state.resultados_auditoria:
                ra = st.session_state.resultados_auditoria
                c1, c2 = st.columns(2)
                c1.metric("🎯 Win Rate Global", f"{ra['w_rate']:.1f}%")
                c2.metric("🔥 Cohetes (+5%)", ra['cohetes'])
                st.markdown("---")
                cols = st.columns(4)
                tits = ["⏸️ Pendiente", "🏆 Éxitos", "⏳ Cuarentena", "🪦 Fracasos"]
                lists = [ra["pendiente"], ra["exitos"], ra["cuarentena"], ra["fracasos"]]
                cols_color = ["#bdc3c7", "#228B22", "#f39c12", "#FF3333"]
                for i, l in enumerate(lists):
                    with cols[i]:
                        st.markdown(f'<h4 title="Nota informativa" style="cursor:help;">{tits[i]}</h4>', unsafe_allow_html=True)
                        for item in l:
                            st.markdown(f"""
                            <div style="border-top:3px solid {cols_color[i]}; background:white; padding:10px; border-radius:8px; margin-bottom:10px; box-shadow:0 2px 5px rgba(0,0,0,0.05);">
                                <div style="display:flex; justify-content:space-between; align-items:center;">
                                    <div><b>{item['T']}</b> <span style="font-size:11px; color:#7f8c8d;">{item['N']}</span></div>
                                    <b style="color:{cols_color[i]};">{item['R']:+.2f}%</b>
                                </div>
                                <div style="font-size:10px; color:#888; margin-top:4px;">Entrada: {item['F']}</div>
                                <div style="font-size:11px; color:#444; margin-top:4px;">In: <b>{item['E']:.2f}{item['M']}</b> | Actual: <b>{item['A']:.2f}{item['M']}</b></div>
                                <div style="font-size:10px; margin-top:6px; color:#1E90FF; font-weight:bold;">{item['KPI']}</div>
                            </div>
                            """, unsafe_allow_html=True)
