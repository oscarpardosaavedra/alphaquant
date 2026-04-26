import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import datetime
import pytz
import gspread
from google.oauth2.service_account import Credentials
import time
import requests
import re

# ==========================================
# 1. CONFIGURACIÓN Y CONEXIÓN DB (BASE DE DATOS)
# ==========================================
st.set_page_config(page_title="Alphaquant", page_icon="📈", layout="wide")
# --- SISTEMA ADMIN Y NUEVA CONEXIÓN ---
PIN_PROCURADO = "197519" 

def conectar_ws(nombre_hoja):
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_dict = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        return client.open("Alphaquant_DB").worksheet(nombre_hoja)
    except:
        return None

with st.sidebar:
    st.markdown("<h2 style='text-align: center;'>🔐 Acceso Admin</h2>", unsafe_allow_html=True)
    pin_ingresado = st.text_input("Introduce tu PIN para ver tu cartera privada:", type="password")
    es_admin = (pin_ingresado == PIN_PROCURADO)
    if es_admin: st.success("✅ Modo Administrador Activo")
    else: st.info("Modo Público: Solo herramientas de análisis.")

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
        return None

# ==========================================
# 2. ESTILOS CSS (TITANIUM DESIGN)
# ==========================================
st.markdown("""
<style>
    [data-testid="stMetric"] { 
        background-color: #f8f9fa; 
        border-radius: 10px; 
        padding: 10px; 
        box-shadow: 0 4px 6px rgba(0,0,0,0.05); 
    }
    [data-testid="stMetricValue"] {
        font-size: 1.8rem !important; 
    }
    [data-testid="stMetricDelta"] {
        font-size: 1.2rem !important;
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
# 3. BASE DE DATOS DE TICKERS
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
    "BIO": "BioRad", "MTD": "Mettler", "TECH": "BioTechne", "CTLT": "Catalent", "ZPTA": "Zapata"
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

def guardar_foto_cartera(total_actual):
    ws_ev = conectar_ws("Evolucion")
    if ws_ev:
        try:
            valor_actual = round(float(total_actual), 2)
            # Traemos las columnas para comparar
            fechas_registradas = ws_ev.col_values(1)
            
            tz = pytz.timezone('Europe/Madrid')
            hoy = datetime.datetime.now(tz).strftime("%Y-%m-%d")
            
            if hoy in fechas_registradas:
                # YA EXISTE HOY: Buscamos dónde está y machacamos el valor
                # El índice en Google Sheets empieza en 1
                fila_index = fechas_registradas.index(hoy) + 1
                ws_ev.update_cell(fila_index, 2, valor_actual)
            else:
                # DÍA NUEVO: Añadimos una fila nueva al final
                ws_ev.append_row([hoy, valor_actual])
        except Exception as e:
            pass

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

nombres_tabs = ["🔬 Análisis Individual", "⚔️ Comparativa", "🎯 Radar Oppenheimer", "🏆 Sala de Trofeos"]
if es_admin:
    nombres_tabs += ["💼 Mi Cartera", "🗓️ Cierres Anuales"]

tabs = st.tabs(nombres_tabs)
tab1, tab2, tab3, tab4 = tabs[0], tabs[1], tabs[2], tabs[3]

# ------------------------------------------
# PESTAÑA 1: VISOR DE GRÁFICOS
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
    st.write("Selecciona varios activos para ver cuál está rindiendo mejor en un mismo periodo de tiempo. Todos empezarán en Base 0 (0% de rendimiento).")
    
    col_comp1, col_comp2 = st.columns([3, 1])
    
    with col_comp1:
        opciones_comp = ["SPY (S&P 500)", "QQQ (Nasdaq 100)"] + opciones_desplegable
        seleccionados = st.multiselect(
            "Elige los activos a enfrentar:", 
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
# PESTAÑA 3: RADAR DE CAZA (MOTOR "OPPENHEIMER" V3.0 - CAZADOR DE ÉLITE)
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
        
        if mercado_objetivo == "EEUU" and "Cerrado" in us["estado"]: st.warning("⚠️ **Aviso:** Wall Street está cerrado.")
        elif mercado_objetivo == "Europa" and "Cerrado" in eu["estado"]: st.warning("⚠️ **Aviso:** Mercado europeo cerrado.")
        elif mercado_objetivo == "Asia" and "Cerrado" in asia["estado"]: st.warning("⚠️ **Aviso:** Mercado asiático cerrado.")

        tickers_a_escanear = [t for t in tickers_nombres.keys() if mercado_objetivo == "Todos" or obtener_region(t) == mercado_objetivo]
        
        mensaje_estado = st.empty()
        mensaje_estado.info(f"🚀 Ejecutando Radar Oppenheimer para: **{mercado_objetivo}**...")
        
        barra_progreso = st.progress(0, text="Analizando ADN financiero de las empresas...")
        resultados_temporales = []
        
        # 1. Calibración Benchmark Global
        alphaSPY_1m, alphaEU_1m, alphaASIA_1m = 0, 0, 0
        try:
            spy_data = yf.download("SPY", period="1y", progress=False)
            if isinstance(spy_data.columns, pd.MultiIndex): spy_data.columns = spy_data.columns.get_level_values(0)
            spy_c = spy_data['Close'].dropna()
            alphaSPY_1m = ((float(spy_c.iloc[-1]) / float(spy_c.iloc[-21])) - 1) * 100
            
            eu_data = yf.download("^STOXX", period="1y", progress=False)
            if isinstance(eu_data.columns, pd.MultiIndex): eu_data.columns = eu_data.columns.get_level_values(0)
            eu_c = eu_data['Close'].dropna()
            alphaEU_1m = ((float(eu_c.iloc[-1]) / float(eu_c.iloc[-21])) - 1) * 100
            
            asia_data = yf.download("^N225", period="1y", progress=False)
            if isinstance(asia_data.columns, pd.MultiIndex): asia_data.columns = asia_data.columns.get_level_values(0)
            asia_c = asia_data['Close'].dropna()
            alphaASIA_1m = ((float(asia_c.iloc[-1]) / float(asia_c.iloc[-21])) - 1) * 100
        except: pass

        ws = conectar_db()
        existentes_en_db = []
        if ws:
            try: existentes_en_db = ws.col_values(1)
            except: pass

        # Pre-cargador de divisas
        fx_rates = {}
        mapa_fx = { "€": "EURUSD=X", "¥": "JPYUSD=X", "GBp": "GBPUSD=X", "kr": "SEKUSD=X", "₹": "INRUSD=X" }
        for mon_sim, par_fx in mapa_fx.items():
            try:
                d_fx = yf.download(par_fx, period="1d", progress=False)
                if not d_fx.empty:
                    if isinstance(d_fx.columns, pd.MultiIndex): d_fx.columns = d_fx.columns.get_level_values(0)
                    fx_rates[mon_sim] = float(d_fx['Close'].dropna().iloc[-1])
            except: pass
        
        # --- BUCLE PRINCIPAL DE CAZA ---
        for i, ticker in enumerate(tickers_a_escanear):
            porc = int(((i + 1) / len(tickers_a_escanear)) * 100)
            barra_progreso.progress((i + 1) / len(tickers_a_escanear), text=f"⏳ `Cazando: {ticker.ljust(6)} | {porc}%`")
            
            try:
                sym_y = a_yahoo(ticker)
                data = yf.download(sym_y, period="5y", progress=False)
                if isinstance(data.columns, pd.MultiIndex): data.columns = data.columns.get_level_values(0)
                df = data[['Close', 'High', 'Low', 'Volume']].dropna()
                if len(df) < 200: continue 

                try:
                    data_ahora = yf.download(sym_y, period="1d", interval="1m", progress=False)
                    if not data_ahora.empty:
                        if isinstance(data_ahora.columns, pd.MultiIndex): data_ahora.columns = data_ahora.columns.get_level_values(0)
                        df.iloc[-1, df.columns.get_loc('Close')] = float(data_ahora['Close'].iloc[-1])
                        df.iloc[-1, df.columns.get_loc('Volume')] = float(data_ahora['Volume'].sum())
                        df.iloc[-1, df.columns.get_loc('High')] = float(max(df.iloc[-1]['High'], data_ahora['High'].max()))
                except: pass

                c_hoy = float(df['Close'].iloc[-1])
                c_ayer = float(df['Close'].iloc[-2])
                pct_h = ((c_hoy / c_ayer) - 1) * 100
                vol_h = float(df['Volume'].iloc[-1])
                vol_m = float(df['Volume'].iloc[-20:].mean())
                
                sma50_serie = df['Close'].rolling(window=50).mean()
                sma200_serie = df['Close'].rolling(window=200).mean()
                sma150_serie = df['Close'].rolling(window=150).mean()
                
                sma50, sma200 = float(sma50_serie.iloc[-1]), float(sma200_serie.iloc[-1])
                sma150 = float(sma150_serie.iloc[-1]) if not sma150_serie.isna().all() else sma200
                sma50_prev = float(sma50_serie.iloc[-6])
                
                delta = df['Close'].diff()
                gain = delta.where(delta > 0, 0).rolling(14).mean()
                loss = -delta.where(delta < 0, 0).rolling(14).mean()
                rsi = float(100 - (100 / (1 + (gain / loss))).iloc[-1])
                
                def ret_d(d): return ((c_hoy / float(df['Close'].iloc[-(d+1)])) - 1) * 100 if len(df) > d else 0
                r1m, r6m, r1y, r5y = ret_d(21), ret_d(126), ret_d(252), ret_d(1260)
                
                # --- DETECTORES DE PATRONES ---
                cruce_reciente = False
                proximidad_oro = False
                if sma200:
                    distancia_m = ((sma50 / sma200) - 1) * 100
                    if sma50 > sma200 and sma50_serie.iloc[-2] <= sma200_serie.iloc[-2]:
                        cruce_reciente = True
                    elif sma50 < sma200 and distancia_m > -1.8 and sma50 > sma50_prev:
                        proximidad_oro = True

                max_14 = float(df['High'].iloc[-14:].max())
                min_14 = float(df['Low'].iloc[-14:].min())
                rango_14d = ((max_14 / min_14) - 1) * 100
                vol_5d = float(df['Volume'].iloc[-5:].mean())
                secado_volumen = (vol_5d < (vol_m * 0.8)) 
                es_compresion = (rango_14d < 6.0 and c_hoy > sma50 and rsi > 50 and secado_volumen)

                es_cohete = (pct_h >= 4.0 and c_hoy > sma50)
                max_52 = float(df['High'].iloc[-252:].max())
                dist_max = ((c_hoy / max_52) - 1) * 100
                isF = (dist_max <= -20 and c_hoy > sma50 and pct_h > 2.0)
                
                # Novedad V3.0: Pocket Pivot & Trend Template
                df_10 = df.iloc[-10:]
                vol_down = df_10[df_10['Close'] < df_10['Close'].shift(1)]['Volume']
                max_vol_down = vol_down.max() if not vol_down.empty else 0
                es_pocket_pivot = (vol_h > max_vol_down) if max_vol_down > 0 else False
                
                es_lider = (c_hoy > sma50 > sma150 > (sma200 if sma200 else 0))
                
                region_act = obtener_region(ticker)
                if region_act == "EEUU": b_1m = alphaSPY_1m
                elif region_act == "Europa": b_1m = alphaEU_1m
                else: b_1m = alphaASIA_1m
                outperformance = r1m - b_1m

                # ==========================================
                # MOTOR DE PUNTUACIÓN V3.0 (SNIPER INSTITUCIONAL)
                # ==========================================
                p = 0
                status_t = "Alcista" if c_hoy > sma50 else "Bajista"
                analisis_parts = []
                
                if es_lider:
                    p += 15
                    analisis_parts.append("🏆 LÍDER REAL: Cumple el 'Trend Template' de Wall Street (Jerarquía de medias perfecta).")
                else:
                    analisis_parts.append(f"Estructura {status_t.lower()} en fase de consolidación.")

                if c_hoy > sma50: p += 10
                if sma200 and c_hoy > sma200: p += 10
                
                if cruce_reciente: 
                    p += 20; analisis_parts.append("✨ 'Cruce de Oro' confirmado: cambio de ciclo estructural.")
                elif proximidad_oro: 
                    p += 12; analisis_parts.append("🎯 Pre-Oro: Las manos fuertes preparan el cruce de medias.")
                
                if es_compresion: 
                    p += 15; analisis_parts.append(f"🤫 VCP: Compresión de precio ({rango_14d:.1f}%) con secado de ventas.")
                
                if es_pocket_pivot and es_compresion:
                    p += 20
                    analisis_parts.append("🎯 POCKET PIVOT: Detectada entrada de volumen masivo oculto dentro del muelle.")
                
                if es_cohete: 
                    puntos_cohete = 15 + min(15, int(pct_h)) 
                    p += puntos_cohete
                    analisis_parts.append(f"🚀 Ruptura de Momentum: El precio escapa con fuerza (+{pct_h:.1f}%).")
                
                if dist_max >= -15.0:
                    p += (5 + int((15 - abs(dist_max)) / 1.5))
                    analisis_parts.append(f"💎 Proximidad a Máximos: Solo un {abs(dist_max):.1f}% le separa de la zona libre.")
                elif dist_max < -40.0 and not isF:
                    p -= 15
                    analisis_parts.append("⚠️ Acción hundida. Peligro de resistencia vendedora estructural.")
                
                if outperformance > 5.0:
                    p += (10 + min(10, int(outperformance / 2)))
                    analisis_parts.append(f"💪 FUERZA RELATIVA: Bate al índice de referencia por un {outperformance:.1f}% este mes.")

                if isF: 
                    puntos_fenix = 35 + min(15, int(abs(dist_max) / 2))
                    p += puntos_fenix
                    analisis_parts.append("🔥 PATRÓN FÉNIX: Intento de resurrección violenta tras castigo severo. Gran oportunidad en suelo.")

                if vol_h > (vol_m * 1.2): 
                    p += (10 + min(10, int((vol_h / vol_m) * 2)))
                    analisis_parts.append(f"🐋 Inyección de volumen institucional ({(vol_h/vol_m):.1f}x).")

                pts = max(0, min(99, int(p)))
                analisis_final_texto = " ".join(analisis_parts)

                # --- ASIGNACIÓN DE ESTRATEGIA DEFINITIVA ---
                est = "⚪ IGNORAR"
                if pts >= 88:
                    if isF: est = "🔥 FÉNIX (Rebote)"
                    elif cruce_reciente: est = "✨ ORO (Élite)"
                    elif es_pocket_pivot and es_compresion: est = "🎯 POCKET PIVOT"
                    elif es_cohete: est = "⚡ MOMENTUM (Cohete)"
                    elif es_compresion: est = "🤫 ACECHO (Muelle)"
                    else: est = "💎 ALFA (Fuerte)"
                elif pts >= 78: 
                    if es_cohete: est = "⚡ MOMENTUM (Fase 1)"
                    elif es_compresion: est = "🤫 ACECHO (Vigilancia)"
                    else: est = "🟢 ACUMULAR"
                elif pts >= 70: 
                    est = "🟡 VIGILAR"

                # Guardado en Base de Datos
                if pts >= 88 and ticker not in existentes_en_db and ws:
                    ws.append_row([ticker, tickers_nombres[ticker], datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), float(c_hoy), int(pts)])
                    existentes_en_db.append(ticker)

                mon = obtener_simbolo_moneda(ticker)
                tr_vals = pd.concat([df['High']-df['Low'], np.abs(df['High']-df['Close'].shift()), np.abs(df['Low']-df['Close'].shift())], axis=1).max(axis=1)
                atr_v = float(tr_vals.rolling(14).mean().iloc[-1])
                stop_v = c_hoy - (atr_v * 2.5)
                
                t_p, t_s = "", ""
                if mon != "$" and fx_rates.get(mon):
                    f = fx_rates[mon] / 100 if mon == "GBp" else fx_rates[mon]
                    t_p = f" (≈ {(c_hoy * f):.2f} $)"
                    t_s = f" (≈ {(stop_v * f):.2f} $)"

                resultados_temporales.append({
                    "TICKER": ticker,
                    "NOMBRE": tickers_nombres[ticker],
                    "PUNTOS": pts,
                    "🎯 SETUP": est,
                    "RSI": f"{rsi:.1f}",
                    "VOL. x": f"{(vol_h/vol_m):.1f}x",
                    "PRECIO": f"{c_hoy:.2f}{mon}{t_p}",
                    "STOP LOSS": f"{stop_v:.2f}{mon}{t_s}",
                    "% HOY": f"{pct_h:+.2f}%", 
                    "% 1 MES": f"{r1m:+.2f}%", 
                    "% 6 MESES": f"{r6m:+.2f}%", 
                    "% 1 AÑO": f"{r1y:+.2f}%", 
                    "% 5 AÑOS": f"{r5y:+.2f}%", 
                    "ANÁLISIS": analisis_final_texto
                })
            except: continue
            
        barra_progreso.empty()
        mensaje_estado.empty()
        st.session_state.resultados_radar = resultados_temporales
        st.session_state.mercado_cazado = mercado_objetivo

    # --- DIBUJADO DE RESULTADOS ---
    if st.session_state.resultados_radar is not None:
        st.success(f"🎯 Caza terminada para: **{st.session_state.get('mercado_cazado', 'Todos')}**")
        
        if not st.session_state.resultados_radar:
            st.info("🕵️‍♂️ Sin activos detectados con la puntuación mínima.")
        else:
            df_res = pd.DataFrame(st.session_state.resultados_radar)
            df_res = df_res.sort_values(by="PUNTOS", ascending=False).reset_index(drop=True)
            
            st.dataframe(df_res.style.map(color_pct, subset=["% HOY", "% 1 MES", "% 6 MESES", "% 1 AÑO", "% 5 AÑOS"]), 
                         use_container_width=False, 
                         width=2400, 
                         height=600, 
                         hide_index=True,
                         column_config={
                            "TICKER": st.column_config.TextColumn(help="Símbolo oficial de la empresa en la bolsa."),
                            "NOMBRE": st.column_config.TextColumn(help="Nombre comercial."),
                            "PUNTOS": st.column_config.ProgressColumn("Rating IA", help="Nota global (0-100). >88 es recomendación clara de compra.", min_value=0, max_value=100, format="%d"),
                            "🎯 SETUP": st.column_config.TextColumn("🎯 ESTRATEGIA", help="""
🎯 POCKET PIVOT: Entrada de dinero institucional oculto. ¡Ojo de halcón!
⚡ MOMENTUM (Cohete): Ruptura alcista explosiva y confirmada hoy.
⚡ MOMENTUM (Fase 1): El valor está despertando hoy. Ideal para vigilancia.
🤫 ACECHO (Muelle): Máxima compresión de precio. Explosión inminente.
🤫 ACECHO (Vigilancia): Comprimiéndose, pero en una estructura aún algo débil.
💎 ALFA (Fuerte): Acción LÍDER. Sube de forma constante, segura e imparable (Tren Bala).
✨ ORO (Élite): Cruce de Medias (50 sobre 200). Ciclo alcista de largo plazo.
⏳ PRE-ORO (Anticipar): Las medias están a punto de cruzarse. Anticipación.
🔥 FÉNIX (Rebote): Resurrección violenta tras una caída del -20% o más.
🟢 ACUMULAR: Tendencia sana y tranquila. Para comprar sin prisas.
"""),
                            "RSI": st.column_config.TextColumn("RSI", help="Termómetro de fuerza relativa. Entre 55 y 68 es la zona dorada de crecimiento."),
                            "VOL. x": st.column_config.TextColumn("Volumen x", help="Volumen hoy vs la media del último mes. Si es mayor a 1.2x, el mercado está respaldando la subida."),
                            "PRECIO": st.column_config.TextColumn("Precio Actual", help="Cotización y conversión estimada a USD."),
                            "STOP LOSS": st.column_config.TextColumn("Stop Loss", help="Nivel sugerido por volatilidad (ATR) para automatizar tu salida en el broker en caso de que la tendencia falle."),
                            "% HOY": st.column_config.TextColumn("Hoy", help="Rendimiento de hoy."),
                            "% 1 MES": st.column_config.TextColumn("1 Mes", help="Rendimiento del último mes."),
                            "% 6 MESES": st.column_config.TextColumn("6 Meses", help="Rendimiento semestral."),
                            "% 1 AÑO": st.column_config.TextColumn("1 Año", help="Rendimiento anual."),
                            "% 5 AÑOS": st.column_config.TextColumn("5 Años", help="Rendimiento a largo plazo."),
                            "ANÁLISIS": st.column_config.TextColumn("Reporte del Algoritmo", width="large", help="Resumen elaborado de la estructura de mercado actual generada por la IA Oppenheimer.")
                         })

# ------------------------------------------
# PESTAÑA 4: SALA DE TROFEOS (PERSISTENTE)
# ------------------------------------------
with tab4:
    st.markdown("### 🏆 Sala de Trofeos")
    st.write("Auditoría persistente. Los datos se mantienen hasta que decidas re-auditar.")
    
    ws = conectar_db()
    if ws is not None:
        try:
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
        except:
            st.error("Error leyendo la base de datos. Comprueba la pestaña de Google Sheets.")

# ==========================================
# 5. PESTAÑAS PRIVADAS (MODO ADMIN)
# ==========================================
if es_admin:
# --- PESTAÑA MI CARTERA (CARGA AUTOMÁTICA CON GRÁFICOS Y ESCUDO) ---
    with tabs[4]:
        st.markdown("### 💼 Centro de Control: Mi Cartera")
        
        ws_c = conectar_ws("Cartera")
        if ws_c:
            try:
                datos_raw = ws_c.get_all_records()
                if datos_raw:
                    tasa_eur_usd = 1.08 
                    try:
                        df_fx = yf.download("EURUSD=X", period="1d", progress=False)
                        if not df_fx.empty:
                            if isinstance(df_fx.columns, pd.MultiIndex): df_fx.columns = df_fx.columns.get_level_values(0)
                            tasa_eur_usd = float(df_fx['Close'].iloc[-1])
                    except: pass

                    lista_val = []; total_inv_eur = 0; total_actual_eur = 0
                    
                    def extraer_numero(valor):
                        s_str = str(valor)
                        if ',' in s_str and '.' in s_str:
                            s_str = s_str.replace('.', '').replace(',', '.')
                        else:
                            s_str = s_str.replace(',', '.')
                        m = re.search(r'([-]?\d+\.?\d*)', s_str)
                        return float(m.group(1)) if m else 0.0

                    for d in datos_raw:
                        try:
                            k_tk = next((k for k in d.keys() if 'ticker' in str(k).lower() or 'activo' in str(k).lower()), None)
                            if not k_tk: continue
                            tk = str(d[k_tk]).strip().upper()
                            if not tk: continue
                            
                            k_brk = next((k for k in d.keys() if 'broker' in str(k).lower()), None)
                            broker_db = str(d[k_brk]) if k_brk else ""
                            es_usd = ("($)" in broker_db or "REVOLUT" in broker_db.upper() or "IBKR" in broker_db.upper())
                            
                            k_precio = next((k for k in d.keys() if 'precio' in str(k).lower()), None)
                            k_cant = next((k for k in d.keys() if 'acc' in str(k).lower() or 'cant' in str(k).lower()), None)
                            k_emp = next((k for k in d.keys() if 'empresa' in str(k).lower()), None)
                            k_fec = next((k for k in d.keys() if 'fecha' in str(k).lower()), None)
                            
                            p_compra = extraer_numero(d[k_precio]) if k_precio else 0.0
                            cant = extraer_numero(d[k_cant]) if k_cant else 0.0
                            
                            tk_y = a_yahoo(tk)
                            hist = yf.download(tk_y, period="5d", progress=False)
                            if isinstance(hist.columns, pd.MultiIndex): hist.columns = hist.columns.get_level_values(0)
                            
                            moneda_nativa = obtener_simbolo_moneda(tk)
                            
                            if not hist.empty and 'Close' in hist.columns:
                                p_actual_nativo = float(hist['Close'].dropna().iloc[-1])
                            else:
                                p_actual_nativo = p_compra
                                
                            # --- TRADUCTOR INTELIGENTE DE DIVISAS ---
                            if moneda_nativa == "$" and not es_usd:
                                # Acción de EEUU pero broker europeo (ej. Trade Republic) -> Pasamos precio Yahoo a Euros
                                p_actual = p_actual_nativo / tasa_eur_usd
                            elif moneda_nativa != "$" and es_usd:
                                # Acción europea pero broker en USD -> Pasamos precio Yahoo a Dólares
                                p_actual = p_actual_nativo * tasa_eur_usd
                            else:
                                p_actual = p_actual_nativo
                                
                            inv_l = p_compra * cant
                            act_l = p_actual * cant
                            
                            inv_e = (inv_l / tasa_eur_usd) if es_usd else inv_l
                            act_e = (act_l / tasa_eur_usd) if es_usd else act_l
                            
                            total_inv_eur += inv_e
                            total_actual_eur += act_e
                            
                            rent_val = ((p_actual/p_compra)-1)*100 if p_compra > 0 else 0
                            
                            lista_val.append({
                                "TICKER": tk,
                                "EMPRESA": str(d[k_emp]) if k_emp else tk,
                                "FECHA": str(d[k_fec]) if k_fec else "",
                                "BROKER": broker_db.split(" ")[0] if broker_db else "N/A",
                                "CANT.": cant,
                                "P. COMPRA": f"{p_compra:,.2f} {'$' if es_usd else '€'}",
                                "P. ACTUAL": f"{p_actual:,.2f} {'$' if es_usd else '€'}",
                                "INVERTIDO": f"{inv_l:,.2f} {'$' if es_usd else '€'}",
                                "VALOR ACTUAL": f"{act_l:,.2f} {'$' if es_usd else '€'}",
                                "GANANCIA (€)": act_e - inv_e,
                                "RENT (%)": rent_val,
                                "INV_E": inv_e
                            })
                        except Exception:
                            continue
                    
                    st.session_state.datos_cartera = lista_val
                    st.session_state.tot_inv = total_inv_eur
                    st.session_state.tot_act = total_actual_eur
                else:
                    st.session_state.datos_cartera = []
            except:
                st.session_state.datos_cartera = []

        col_c_izq, col_c_der = st.columns([1, 3])
        
        with col_c_izq:
            st.markdown("#### 🛒 Registrar Compra")
            with st.form("form_compra"):
                tk_c = st.selectbox("Activo:", opciones_desplegable)
                broker_c = st.selectbox("Broker / Moneda:", ["Trade Republic (€)", "Revolut ($)", "Interactive Brokers ($)", "Scalable Capital (€)", "Otro (€)", "Otro ($)"])
                total_c = st.number_input("Capital Total Invertido:", min_value=0.01, step=100.0)
                cant_c = st.number_input("Nº de Acciones recibidas:", min_value=0.000001, step=1.0)
                fecha_c = st.date_input("Fecha de Compra:")
                
                if st.form_submit_button("Añadir a Cartera", use_container_width=True):
                    if ws_c:
                        try:
                            t_limpio = tk_c.split(" ")[0]
                            # HORA MADRID
                            ahora = datetime.datetime.now(pytz.timezone('Europe/Madrid')).strftime("%H:%M")
                            fecha_con_hora = f"{fecha_c} {ahora}"
                            
                            ws_c.append_row([
                                t_limpio, 
                                tickers_nombres.get(t_limpio, t_limpio), 
                                float(cant_c), 
                                round(float(total_c / cant_c), 4), 
                                fecha_con_hora,
                                broker_c
                            ])
                            st.success(f"✅ Registrado con éxito a las {ahora}.")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e: st.error(f"Error al guardar en Google Sheets: {e}")

            st.markdown("#### 🗑️ Corregir / Borrar")
            with st.expander("Eliminar operación de la Cartera"):
                if st.session_state.get('datos_cartera'):
                    opciones_borrar = [f"{d['TICKER']} | {d['FECHA']} | {d['CANT.']} uds" for d in st.session_state.datos_cartera]
                    with st.form("form_borrar_cartera"):
                        op_elegida = st.selectbox("Selecciona la operación:", opciones_borrar)
                        if st.form_submit_button("Eliminar Registro", use_container_width=True):
                            if ws_c:
                                partes = op_elegida.split(" | ")
                                ticker_b = partes[0].strip()
                                fecha_b = partes[1].strip()
                                
                                celdas_tk = ws_c.findall(ticker_b, in_column=1)
                                fila_a_borrar = None
                                for celda in celdas_tk:
                                    if str(ws_c.cell(celda.row, 5).value).strip() == fecha_b:
                                        fila_a_borrar = celda.row
                                        break
                                
                                if fila_a_borrar:
                                    ws_c.delete_rows(fila_a_borrar)
                                    st.success(f"🗑️ Registro eliminado.")
                                    time.sleep(1)
                                    st.rerun()
                else:
                    st.info("No hay acciones registradas.")

        with col_c_der:
            if st.session_state.get('datos_cartera') and len(st.session_state.datos_cartera) > 0:
                df_c = pd.DataFrame(st.session_state.datos_cartera)
                inv_e = st.session_state.tot_inv
                act_e = st.session_state.tot_act
                gan_e = act_e - inv_e
                
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("💰 Valor Cartera", f"{act_e:,.2f} €")
                c2.metric("📥 Invertido", f"{inv_e:,.2f} €")
                c3.metric("🚀 Ganancia Real", f"{gan_e:+,.2f} €")
                c4.metric("📈 Rent. Global", f"{((gan_e/inv_e)*100 if inv_e>0 else 0):+.2f}%")
                
                st.markdown("---")
                st.markdown("#### 📋 Posiciones Actuales")
                
                def color_numeros(val):
                    if isinstance(val, (int, float)):
                        if val > 0: return 'color: #228B22; font-weight: bold;'
                        elif val < 0: return 'color: #FF3333; font-weight: bold;'
                    return ''
                
                st.dataframe(
                    df_c[['TICKER', 'EMPRESA', 'FECHA', 'BROKER', 'CANT.', 'P. COMPRA', 'P. ACTUAL', 'INVERTIDO', 'VALOR ACTUAL', 'GANANCIA (€)', 'RENT (%)']].style.map(color_numeros, subset=['GANANCIA (€)', 'RENT (%)']),
                    use_container_width=True, hide_index=True,
                    column_config={
                        "TICKER": st.column_config.TextColumn("TICKER", help="Código bursátil del activo."),
                        "GANANCIA (€)": st.column_config.NumberColumn("GANANCIA (€)", format="%+.2f €"),
                        "RENT (%)": st.column_config.NumberColumn("RENT (%)", format="%+.2f%%")
                    }
                )

                st.markdown("<br>", unsafe_allow_html=True)
                
                c_g1, c_g2 = st.columns(2)
                with c_g1:
                    try:
                        fig_pie = px.pie(df_c, values='INV_E', names='TICKER', title='Distribución de Capital (€)', hole=0.4)
                        st.plotly_chart(fig_pie, use_container_width=True)
                    except: pass
                
                with c_g2:
                    try:
                        # 1. Agrupamos todas las compras por Ticker y sumamos sus ganancias
                        df_agrupado = df_c.groupby('TICKER', as_index=False)['GANANCIA (€)'].sum()
                        
                        # 2. Decidimos el color de la barra en base al Total Neto
                        df_agrupado['Color'] = np.where(df_agrupado['GANANCIA (€)'] >= 0, 'green', 'red')
                        
                        # 3. Dibujamos la gráfica limpia
                        fig_bar = px.bar(df_agrupado, x='TICKER', y='GANANCIA (€)', 
                                         title='Ganancia Neta por Activo (€)', 
                                         color='Color', 
                                         color_discrete_map={'green':'#228B22', 'red':'#FF3333'})
                        
                        fig_bar.update_layout(
                            showlegend=False, 
                            template="plotly_dark", 
                            paper_bgcolor='rgba(0,0,0,0)', 
                            plot_bgcolor='rgba(0,0,0,0)'
                        )
                        st.plotly_chart(fig_bar, use_container_width=True)
                    except Exception as e: 
                        st.error(f"Error en gráfica de barras: {e}")

            else:
                st.info("No hay nada en la cartera.")

# --- LÓGICA DE EVOLUCIÓN HISTÓRICA AUTOMÁTICA ---
# --- LÓGICA DE EVOLUCIÓN E IMPACTO DIARIO (SÍN QUITAR NADA) ---
        if st.session_state.get('tot_act'):
            guardar_foto_cartera(st.session_state.tot_act)
            
            st.markdown("---")
            ws_ev = conectar_ws("Evolucion")
            if ws_ev:
                try:
                    datos_ev = ws_ev.get_all_records()
                    if len(datos_ev) > 0:
                        df_ev = pd.DataFrame(datos_ev)
                        df_ev.columns = [str(c).strip().capitalize() for c in df_ev.columns]
                        df_ev['Fecha'] = pd.to_datetime(df_ev['Fecha'])
                        df_ev = df_ev.sort_values('Fecha')
                        
                        # Limpieza de valores decimales
                        if df_ev['Valor'].dtype == object:
                            df_ev['Valor'] = df_ev['Valor'].astype(str).str.replace(',', '.').str.extract(r'([-]?\d+\.?\d*)').astype(float)

                        # --- 1. MÉTRICAS DE VARIACIÓN (KPIs) ---
                        if len(df_ev) > 1:
                            df_ev['Var_Pct'] = df_ev['Valor'].pct_change() * 100
                            ultima_var = df_ev['Var_Pct'].iloc[-1]
                            ultima_var_euro = df_ev['Valor'].iloc[-1] - df_ev['Valor'].iloc[-2]
                            
                            st.markdown("#### ⚡ Rendimiento de la Última Sesión")
                            cv1, cv2, cv3 = st.columns([1, 1, 2])
                            cv1.metric("Variación (%)", f"{ultima_var:+,.2f}%")
                            cv2.metric("Variación (€)", f"{ultima_var_euro:+,.2f} €")
                            cv3.caption("Evolución calculada respecto al registro anterior en tu historial.")

                            # --- 2. GRÁFICA DE VOLATILIDAD (BARRAS) ---
                            df_ev['Color'] = np.where(df_ev['Var_Pct'] >= 0, '#228B22', '#FF3333')
                            fig_var = px.bar(df_ev.dropna(subset=['Var_Pct']), x='Fecha', y='Var_Pct',
                                             title="Retornos Diarios (%)",
                                             labels={'Var_Pct': 'Variación (%)', 'Fecha': 'Día'},
                                             color='Color', color_discrete_map="identity")
                            fig_var.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', height=300)
                            st.plotly_chart(fig_var, use_container_width=True)

                        # --- 3. GRÁFICA DE VALOR TOTAL (ÁREA - LA DE SIEMPRE) ---
                        st.markdown("#### 📈 Evolución Histórica del Capital Total")
                        fig_evolucion = px.area(df_ev, x='Fecha', y='Valor', 
                                                title="Crecimiento del Patrimonio en Euros (€)",
                                                labels={'Valor': 'Total (€)', 'Fecha': 'Día'},
                                                color_discrete_sequence=['#228B22'])
                        
                        fig_evolucion.update_layout(
                            hovermode="x unified",
                            template="plotly_dark",
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                            yaxis=dict(gridcolor='rgba(255,255,255,0.1)', tickformat=",.2f")
                        )
                        fig_evolucion.update_traces(mode="lines+markers", marker=dict(size=6))
                        st.plotly_chart(fig_evolucion, use_container_width=True)

                    else:
                        st.info("💡 La gráfica se generará cuando el sistema tenga datos registrados.")
                except Exception as e:
                    st.error(f"Error en el panel de evolución: {e}")

    # --- ANÁLISIS DE RIESGO Y LIQUIDEZ (SECTORES Y BROKERS) ---
        st.markdown("---")
        st.markdown("### 🔍 Análisis de Diversificación y Riesgo")
        
        c_risk1, c_risk2 = st.columns(2)
        
        with c_risk1:
            # 1. DIVERSIFICACIÓN POR SECTOR
            st.markdown("#### 🏢 Reparto por Sectores")
            sectores_dict = {}
            
            with st.spinner("Analizando sectores..."):
                for item in st.session_state.datos_cartera:
                    tk = item['TICKER']
                    inv_e = item['INV_E']
                    try:
                        # Consultamos a Yahoo el sector de la empresa
                        ticker_info = yf.Ticker(a_yahoo(tk)).info
                        sector = ticker_info.get('sector', 'Otros')
                    except:
                        sector = 'Desconocido'
                    
                    sectores_dict[sector] = sectores_dict.get(sector, 0) + inv_e
            
            df_sectores = pd.DataFrame(list(sectores_dict.items()), columns=['Sector', 'Inversión (€)'])
            
            # Gráfico Sunburst (muy pro para ver jerarquías) o Pie
            fig_sectores = px.pie(df_sectores, values='Inversión (€)', names='Sector', 
                                  hole=0.4,
                                  color_discrete_sequence=px.colors.qualitative.Safe)
            
            fig_sectores.update_layout(showlegend=True, template="plotly_dark", 
                                       paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_sectores, use_container_width=True)
            st.caption("Muestra cuánto capital tienes expuesto a cada industria.")

        with c_risk2:
            # 2. EXPOSICIÓN POR BROKER
            st.markdown("#### 🏦 Ubicación del Capital")
            broker_dict = {}
            
            for item in st.session_state.datos_cartera:
                # Limpiamos el nombre del broker (quitamos el símbolo de moneda si existe)
                brk = item['BROKER'].split(" (")[0]
                inv_e = item['INV_E']
                broker_dict[brk] = broker_dict.get(brk, 0) + inv_e
            
            df_brokers = pd.DataFrame(list(broker_dict.items()), columns=['Broker', 'Total (€)'])
            
            fig_brokers = px.pie(df_brokers, values='Total (€)', names='Broker', 
                                 hole=0.6, # Tipo Donut
                                 color_discrete_sequence=px.colors.sequential.Greens_r)
            
            fig_brokers.update_layout(showlegend=True, template="plotly_dark", 
                                      paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_brokers, use_container_width=True)
            st.caption("Distribución física de tu dinero entre tus diferentes plataformas.")

    # --- PESTAÑA CIERRES ANUALES ---
    with tabs[5]:
        st.subheader("🗓️ Histórico de Cierres y Rendimiento Anual")
        
        dict_compras = {}
        dict_compras_info = {} 
        
        if st.session_state.get('datos_cartera'):
            for item in st.session_state.datos_cartera:
                tk = item.get("TICKER", "").upper()
                cant = float(item.get("CANT.", 0.0))
                inv_e = float(item.get("INV_E", 0.0)) 
                broker = item.get("BROKER", "N/A")
                empresa = item.get("EMPRESA", tk)
                fecha_compra = item.get("FECHA", "")
                
                if tk and cant > 0:
                    if tk not in dict_compras:
                        dict_compras[tk] = 0.0
                        dict_compras_info[tk] = {'inv_e_total': 0.0, 'broker': broker, 'empresa': empresa, 'fecha_compra': fecha_compra}
                    
                    dict_compras[tk] += cant
                    dict_compras_info[tk]['inv_e_total'] += inv_e
        else:
            ws_inventario = conectar_ws("Cartera")
            if ws_inventario:
                try:
                    data_inv = ws_inventario.get_all_records()
                    if data_inv:
                        for d in data_inv:
                            d_l = {str(k).lower().replace(' ', '').replace('_', '').replace('º', '').replace('.', ''): v for k, v in d.items()}
                            tk = str(d_l.get('ticker', d_l.get('activo', ''))).strip().upper()
                            raw_cant = str(d_l.get('cantidad', d_l.get('cant', d_l.get('nacciones', 0)))).replace(',', '.')
                            
                            fec_key = next((k for k in d.keys() if 'fecha' in str(k).lower()), None)
                            fecha_c = str(d[fec_key]) if fec_key else ""
                            
                            brk_key = next((k for k in d.keys() if 'broker' in str(k).lower()), None)
                            broker_c = str(d[brk_key]) if brk_key else "N/A"
                            
                            emp_key = next((k for k in d.keys() if 'empresa' in str(k).lower()), None)
                            empresa_c = str(d[emp_key]) if emp_key else tk
                            
                            try: cant = float(raw_cant) if raw_cant else 0.0
                            except: cant = 0.0
                            
                            if tk and cant > 0: 
                                if tk not in dict_compras:
                                    dict_compras[tk] = 0.0
                                    dict_compras_info[tk] = {'inv_e_total': 0.0, 'broker': broker_c, 'empresa': empresa_c, 'fecha_compra': fecha_c}
                                dict_compras[tk] += cant
                except Exception: pass

        dict_stock = {}
        for tk, cant_comprada in dict_compras.items():
            if cant_comprada > 0.0001:
                dict_stock[tk] = round(cant_comprada, 4)

        ws_cierres = conectar_ws("Cierres")
        datos_cierres_auto = [] 
        if ws_cierres:
            try:
                datos_cierres_auto = ws_cierres.get_all_records()
            except Exception: pass

        col_cl1, col_cl2 = st.columns([1, 3])
        
        with col_cl1:
            st.markdown("#### 🤝 Registrar Venta")
            
            if not dict_stock:
                st.warning("⚠️ No tienes acciones registradas en cartera para poder vender.")
            else:
                with st.form("form_cierre_validado"):
                    tk_v = st.selectbox("Activo a Vender:", sorted(list(dict_stock.keys())))
                    cant_v = st.number_input("Cantidad a vender:", min_value=0.0001, step=1.0)
                    total_venta_local = st.number_input("Total Obtenido por la Venta (€/$):", min_value=0.0, step=10.0, help="Importe bruto recibido por la venta en la moneda de tu broker.")
                    fecha_v = st.date_input("Fecha de Venta:")
                    
                    if st.form_submit_button("Registrar Cierre Automático", use_container_width=True):
                        max_permitido = dict_stock.get(tk_v, 0.0)
                        
                        if cant_v > max_permitido:
                            st.error(f"❌ Operación bloqueada. Intentas vender {cant_v} uds de {tk_v}, pero solo posees {max_permitido}.")
                        else:
                            with st.spinner("Registrando venta y actualizando tu Cartera..."):
                                info_compra = dict_compras_info[tk_v]
                                empresa_v = info_compra['empresa']
                                broker_v = info_compra['broker']
                                total_comprado = dict_compras[tk_v]
                                inv_e_total = info_compra['inv_e_total']
                                fecha_compra_str = info_compra.get('fecha_compra', str(fecha_v))
                                
                                try:
                                    f_compra_str = fecha_compra_str.split(" ")[0]
                                    try:
                                        f_compra_dt = datetime.datetime.strptime(f_compra_str, "%Y-%m-%d").date()
                                    except:
                                        f_compra_dt = pd.to_datetime(f_compra_str, dayfirst=True).date()
                                    dias_cartera = (fecha_v - f_compra_dt).days
                                    if dias_cartera < 0: dias_cartera = 0
                                except:
                                    dias_cartera = 0
                                
                                p_medio_compra_e = inv_e_total / total_comprado if total_comprado > 0 else 0
                                inversion_venta_e = p_medio_compra_e * cant_v
                                
                                es_usd = ("($)" in broker_v or "REVOLUT" in broker_v.upper() or "IBKR" in broker_v.upper())
                                tasa_eur_usd = 1.08 
                                if es_usd:
                                    try:
                                        df_fx = yf.download("EURUSD=X", period="1d", progress=False)
                                        if not df_fx.empty:
                                            if isinstance(df_fx.columns, pd.MultiIndex): df_fx.columns = df_fx.columns.get_level_values(0)
                                            tasa_eur_usd = float(df_fx['Close'].iloc[-1])
                                    except: pass
                                
                                total_venta_e = (total_venta_local / tasa_eur_usd) if es_usd else total_venta_local
                                
                                if inversion_venta_e > 0:
                                    prec_v = ((total_venta_e / inversion_venta_e) - 1) * 100
                                    gan_v = total_venta_e - inversion_venta_e
                                else:
                                    prec_v = 0.0
                                    gan_v = total_venta_e

                                # HORA MADRID PARA LA VENTA
                                hora_v = datetime.datetime.now(pytz.timezone('Europe/Madrid')).strftime("%H:%M")
                                fecha_venta_completa = f"{fecha_v} {hora_v}"

                                if ws_cierres:
                                    ws_cierres.append_row([
                                        tk_v, 
                                        empresa_v, 
                                        round(float(prec_v), 2), 
                                        round(float(gan_v), 2), 
                                        fecha_venta_completa, 
                                        broker_v,
                                        float(cant_v),
                                        int(dias_cartera),
                                        float(total_venta_local)
                                    ])
                                    
                                    # Descontamos de la Cartera
                                    ws_cartera = conectar_ws("Cartera")
                                    if ws_cartera:
                                        celdas_c = ws_cartera.findall(tk_v, in_column=1)
                                        celdas_c.reverse() 
                                        cant_a_descontar = cant_v
                                        
                                        for celda in celdas_c:
                                            if cant_a_descontar <= 0.0001: break
                                            try:
                                                cant_fila = float(str(ws_cartera.cell(celda.row, 3).value).replace(',', '.'))
                                            except: cant_fila = 0.0
                                            
                                            if cant_fila <= cant_a_descontar + 0.0001:
                                                ws_cartera.delete_rows(celda.row)
                                                cant_a_descontar -= cant_fila
                                            else:
                                                ws_cartera.update_cell(celda.row, 3, cant_fila - cant_a_descontar)
                                                cant_a_descontar = 0

                                    st.success(f"✅ Venta registrada a las {hora_v}. Stock actualizado.")
                                    time.sleep(2)
                                    st.rerun()

            st.markdown("#### 🗑️ Gestión de Ventas")
            with st.expander("Corregir / Eliminar Venta"):
                if datos_cierres_auto:
                    opciones_borrar_v = [f"{d.get('Ticker', d.get('Activo', ''))} - {d.get('Fecha de Venta', d.get('Fecha', ''))} ({d.get('Cantidad', '')} uds)" for d in datos_cierres_auto]
                    with st.form("form_borrar_venta"):
                        v_borrar = st.selectbox("Selecciona la venta:", opciones_borrar_v)
                        accion_borrar = st.radio("¿Qué deseas hacer?", [
                            "🔄 Anular Venta (Las acciones vuelven a tu Cartera)",
                            "❌ Eliminar Definitivamente (Solo se borra del historial de Ventas)"
                        ])
                        
                        if st.form_submit_button("Ejecutar Acción", use_container_width=True):
                            if ws_cierres:
                                tk_a_borrar = v_borrar.split(" - ")[0].strip()
                                cell = ws_cierres.find(tk_a_borrar, in_column=1)
                                
                                if cell: 
                                    row_vals = ws_cierres.row_values(cell.row)
                                    ws_cierres.delete_rows(cell.row)
                                    
                                    if "Anular" in accion_borrar:
                                        try:
                                            emp_rest = row_vals[1]
                                            gan_rest = float(str(row_vals[3]).replace(',', '.'))
                                            fec_rest = row_vals[4].split(" ")[0]
                                            brk_rest = row_vals[5]
                                            cant_rest = float(str(row_vals[6]).replace(',', '.'))
                                            tot_rest = float(str(row_vals[8]).replace(',', '.'))
                                            
                                            inv_rest = tot_rest - gan_rest
                                            precio_medio_rest = round(inv_rest / cant_rest, 4) if cant_rest > 0 else 0
                                            
                                            ws_cartera = conectar_ws("Cartera")
                                            if ws_cartera:
                                                hora_r = datetime.datetime.now(pytz.timezone('Europe/Madrid')).strftime("%H:%M")
                                                fecha_r = f"{fec_rest} {hora_r}"
                                                ws_cartera.append_row([
                                                    tk_a_borrar, emp_rest, cant_rest, precio_medio_rest, fecha_r, brk_rest
                                                ])
                                            st.success(f"🔄 Venta anulada. Las {cant_rest} acciones vuelven a tu Cartera.")
                                        except:
                                            st.warning("Venta borrada, pero no se pudo restaurar en la cartera (faltan datos).")
                                    else:
                                        st.success(f"❌ Venta eliminada definitivamente del historial.")
                                        
                                    time.sleep(2)
                                    st.rerun()
                else:
                    st.info("No hay ventas registradas.")

        with col_cl2:
            if datos_cierres_auto and len(datos_cierres_auto) > 0:
                df_cierres = pd.DataFrame(datos_cierres_auto)
                
                col_rent = next((c for c in df_cierres.columns if 'rent' in str(c).lower() or '%' in str(c)), None)
                col_gan = next((c for c in df_cierres.columns if 'ganancia' in str(c).lower() or 'efectiva' in str(c).lower() or 'beneficio' in str(c).lower()), None)
                
                if col_gan and col_rent:
                    try:
                        def clean_numeric(series):
                            s_str = series.astype(str).str.replace(',', '.')
                            s_num = s_str.str.extract(r'([-]?\d+\.?\d*)', expand=False)
                            return pd.to_numeric(s_num, errors='coerce').fillna(0.0)
                            
                        df_kpi = df_cierres.copy()
                        df_kpi[col_gan] = clean_numeric(df_kpi[col_gan])
                        df_kpi[col_rent] = clean_numeric(df_kpi[col_rent])
                        
                        win_rate = (df_kpi[col_gan] > 0).mean() * 100
                        beneficio_neto = df_kpi[col_gan].sum()
                        operaciones_totales = len(df_kpi)
                        operaciones_ganadoras = len(df_kpi[df_kpi[col_gan] > 0])
                        operaciones_perdedoras = operaciones_totales - operaciones_ganadoras
                        
                        c_b1, c_b2, c_b3, c_b4 = st.columns(4)
                        c_b1.metric("💸 Beneficio Neto", f"{beneficio_neto:+,.2f} €")
                        c_b2.metric("🎯 Win Rate", f"{win_rate:.1f}%")
                        c_b3.metric("🏆 Ganadoras", f"{operaciones_ganadoras}")
                        c_b4.metric("🪦 Perdedoras", f"{operaciones_perdedoras}")
                        
                        st.markdown("---")
                    
                        # --- 1. TABLA (ARRIBA) ---
                        st.markdown("#### 📜 Registro de Operaciones Cerradas")
                        df_cierres_mostrar = df_cierres.copy()
                        
                        if col_gan: df_cierres_mostrar[col_gan] = clean_numeric(df_cierres_mostrar[col_gan]).apply(lambda x: f"{x:+,.2f} €")
                        if col_rent: df_cierres_mostrar[col_rent] = clean_numeric(df_cierres_mostrar[col_rent]).apply(lambda x: f"{x:+.2f}%")
                        
                        col_tot = next((c for c in df_cierres_mostrar.columns if 'total' in str(c).lower() or 'obtenido' in str(c).lower()), None)
                        if col_tot: df_cierres_mostrar[col_tot] = clean_numeric(df_cierres_mostrar[col_tot]).apply(lambda x: f"{x:,.2f} €" if pd.notnull(x) else x)

                        cols_buscadas = ["Ticker", "Empresa", "Rentabilidad", "Ganancia", "Fecha", "Broker", "Cantidad", "Días", "Total"]
                        cols_mostrar_final = []
                        
                        for ideal in cols_buscadas:
                            for real in df_cierres_mostrar.columns:
                                if ideal.lower() in str(real).lower() and real not in cols_mostrar_final:
                                    cols_mostrar_final.append(real)
                                    break
                                    
                        for real in df_cierres_mostrar.columns:
                            if real not in cols_mostrar_final:
                                cols_mostrar_final.append(real)
                        
                        def color_celdas(val):
                            if isinstance(val, str) and ('%' in val or '€' in val):
                                if val.startswith('+'): return 'color: #228B22; font-weight: bold;' 
                                elif val.startswith('-'): return 'color: #FF3333; font-weight: bold;' 
                            return ''

                        st.dataframe(df_cierres_mostrar[cols_mostrar_final].style.map(color_celdas), use_container_width=True, hide_index=True)

                        st.markdown("<br>", unsafe_allow_html=True)

                        # --- 2. GRÁFICOS (DEBAJO) ---
                        st.markdown("#### 📊 Gráficos de Rendimiento")
                        c_gc1, c_gc2 = st.columns(2)
                        with c_gc1:
                            if operaciones_totales > 0:
                                df_win_loss = pd.DataFrame({'Resultado': ['Ganadoras', 'Perdedoras'], 'Cantidad': [operaciones_ganadoras, operaciones_perdedoras]})
                                fig_win = px.pie(df_win_loss, values='Cantidad', names='Resultado', title='Tasa de Acierto (Win Rate)', hole=0.4, color='Resultado', color_discrete_map={'Ganadoras':'#228B22', 'Perdedoras':'#FF3333'})
                                st.plotly_chart(fig_win, use_container_width=True)
                            
                        with c_gc2:
                            df_kpi['Color'] = np.where(df_kpi[col_gan] > 0, 'green', 'red')
                            df_kpi['Operacion'] = [f"Op {i+1} ({tk})" for i, tk in enumerate(df_kpi.get('Ticker', df_kpi.get('Activo', df_kpi.index)))]
                            fig_hist = px.bar(df_kpi, x='Operacion', y=col_gan, title='Historial de Trades', color='Color', color_discrete_map={'green':'#228B22', 'red':'#FF3333'})
                            fig_hist.update_layout(showlegend=False, xaxis_title="", yaxis_title="Beneficio Realizado")
                            st.plotly_chart(fig_hist, use_container_width=True)
                            
                    except Exception as e: 
                        st.error(f"Error interno procesando los cierres: {e}")
                else:
                    st.error("⚠️ No se encuentran las columnas de Ganancia o Rentabilidad en el Sheets. Revísalas.")
            else:
                st.info("No hay operaciones cerradas registradas aún.")
