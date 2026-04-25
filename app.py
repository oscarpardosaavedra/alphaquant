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
    # --- PESTAÑA MI CARTERA ---
    with tabs[4]:
        st.markdown("### 💼 Centro de Control: Mi Cartera")
        
        col_c_izq, col_c_der = st.columns([1, 3])
        
        with col_c_izq:
            st.markdown("#### 🛒 Registrar Compra")
            with st.form("form_compra"):
                tk_c = st.selectbox("Activo:", opciones_desplegable)
                cant_c = st.number_input("Cantidad de Acciones:", min_value=0.01, step=1.0)
                prec_c = st.number_input("Precio Medio de Compra:", min_value=0.01, step=0.5)
                fecha_c = st.date_input("Fecha de Compra:")
                if st.form_submit_button("Añadir a Cartera", use_container_width=True):
                    ws_c = conectar_ws("Cartera")
                    if ws_c:
                        t_limpio = tk_c.split(" ")[0]
                        ws_c.append_row([t_limpio, tickers_nombres.get(t_limpio, t_limpio), float(cant_c), float(prec_c), str(fecha_c)])
                        st.success("✅ Compra registrada en Sheets.")
                        time.sleep(1)
                        st.rerun()

            st.markdown("#### 🔄 Actualizar Datos")
            if st.button("Sincronizar con Wall Street", use_container_width=True):
                ws_c = conectar_ws("Cartera")
                if ws_c:
                    try:
                        datos_raw = ws_c.get_all_records()
                        if datos_raw:
                            lista_val = []
                            total_invertido = 0
                            total_actual = 0
                            
                            barra_v = st.progress(0, text="Valorando posiciones en tiempo real...")
                            for i, d in enumerate(datos_raw):
                                try:
                                    # LECTURA A PRUEBA DE FALLOS: Busca múltiples nombres posibles
                                    tk = str(d.get('Ticker', d.get('Activo', ''))).strip()
                                    if not tk: continue
                                    
                                    tk_y = a_yahoo(tk)
                                    hist = yf.download(tk_y, period="5d", progress=False)
                                    if isinstance(hist.columns, pd.MultiIndex): hist.columns = hist.columns.get_level_values(0)
                                    
                                    if hist.empty or 'Close' not in hist.columns: continue
                                    
                                    p_actual = float(hist['Close'].dropna().iloc[-1])
                                    
                                    # Escáner flexible para los precios y cantidades
                                    raw_compra = str(d.get('Precio_Compra', d.get('Precio Compra', 0))).replace(',', '.')
                                    raw_cant = str(d.get('Nº Acciones', d.get('Cantidad', 0))).replace(',', '.')
                                    
                                    p_compra = float(raw_compra)
                                    cant = float(raw_cant)
                                    
                                    if p_compra > 0 and cant > 0:
                                        subtotal_inv = p_compra * cant
                                        subtotal_act = p_actual * cant
                                        ganancia = subtotal_act - subtotal_inv
                                        pct = ((p_actual / p_compra) - 1) * 100
                                        
                                        moneda = obtener_simbolo_moneda(tk)
                                        total_invertido += subtotal_inv
                                        total_actual += subtotal_act
                                        
                                        lista_val.append({
                                            "TICKER": tk,
                                            "EMPRESA": str(d.get('Empresa', '')),
                                            "CANT.": cant,
                                            "PRECIO COMPRA": p_compra,
                                            "PRECIO ACTUAL": p_actual,
                                            "INVERTIDO": subtotal_inv,
                                            "VALOR ACTUAL": subtotal_act,
                                            "GANANCIA LATENTE": ganancia,
                                            "RENT. (%)": pct,
                                            "MONEDA": moneda
                                        })
                                except Exception as inner_e: 
                                    pass # Ignora filas mal escritas pero NO rompe la web
                                barra_v.progress((i+1)/len(datos_raw))
                            
                            st.session_state.datos_cartera = lista_val
                            st.session_state.tot_inv = total_invertido
                            st.session_state.tot_act = total_actual
                            barra_v.empty()
                        else:
                            st.warning("⚠️ La pestaña 'Cartera' está vacía.")
                    except Exception as exc:
                        st.error(f"Error al conectar o leer datos: {exc}")

        with col_c_der:
            if st.session_state.get('datos_cartera') and len(st.session_state.datos_cartera) > 0:
                df_cartera = pd.DataFrame(st.session_state.datos_cartera)
                
                inv = st.session_state.tot_inv
                act = st.session_state.tot_act
                gan_total = act - inv
                pct_total = ((act / inv) - 1) * 100 if inv > 0 else 0
                
                # --- KPIS PRINCIPALES ---
                c_m1, c_m2, c_m3, c_m4 = st.columns(4)
                c_m1.metric("💰 Valor Actual", f"{act:,.2f} €")
                c_m2.metric("📥 Invertido", f"{inv:,.2f} €")
                c_m3.metric("🚀 Beneficio Latente", f"{gan_total:+,.2f} €")
                c_m4.metric("📈 Rentabilidad Total", f"{pct_total:+.2f}%")
                
                st.markdown("---")
                
                # --- GRÁFICOS VISUALES ---
                c_g1, c_g2 = st.columns(2)
                with c_g1:
                    if 'INVERTIDO' in df_cartera.columns and not df_cartera.empty:
                        fig_pie = px.pie(df_cartera, values='INVERTIDO', names='TICKER', title='Distribución (Peso %)', hole=0.4, color_discrete_sequence=px.colors.sequential.Plasma)
                        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                        st.plotly_chart(fig_pie, use_container_width=True)
                with c_g2:
                    if 'GANANCIA LATENTE' in df_cartera.columns and not df_cartera.empty:
                        df_cartera['Color'] = np.where(df_cartera['GANANCIA LATENTE'] > 0, 'green', 'red')
                        fig_bar = px.bar(df_cartera, x='TICKER', y='GANANCIA LATENTE', title='Ganancia/Pérdida por Activo', color='Color', color_discrete_map={'green':'#228B22', 'red':'#FF3333'})
                        fig_bar.update_layout(showlegend=False)
                        st.plotly_chart(fig_bar, use_container_width=True)
                
                # --- TABLA DE POSICIONES ABIERTAS ---
                st.markdown("#### 📋 Posiciones Abiertas")
                df_mostrar = df_cartera.copy()
                df_mostrar['PRECIO COMPRA'] = df_mostrar.apply(lambda x: f"{x['PRECIO COMPRA']:.2f} {x['MONEDA']}", axis=1)
                df_mostrar['PRECIO ACTUAL'] = df_mostrar.apply(lambda x: f"{x['PRECIO ACTUAL']:.2f} {x['MONEDA']}", axis=1)
                df_mostrar['INVERTIDO'] = df_mostrar.apply(lambda x: f"{x['INVERTIDO']:,.2f} €", axis=1)
                df_mostrar['VALOR ACTUAL'] = df_mostrar.apply(lambda x: f"{x['VALOR ACTUAL']:,.2f} €", axis=1)
                df_mostrar['GANANCIA LATENTE'] = df_mostrar.apply(lambda x: f"{x['GANANCIA LATENTE']:+,.2f} €", axis=1)
                df_mostrar['RENT. (%)'] = df_mostrar['RENT. (%)'].apply(lambda x: f"{x:+.2f}%")
                
                df_mostrar = df_mostrar[['TICKER', 'EMPRESA', 'CANT.', 'PRECIO COMPRA', 'PRECIO ACTUAL', 'INVERTIDO', 'VALOR ACTUAL', 'GANANCIA LATENTE', 'RENT. (%)']]
                st.dataframe(df_mostrar.style.map(color_pct, subset=["GANANCIA LATENTE", "RENT. (%)"]), use_container_width=True, hide_index=True)
            else:
                st.info("👈 Pulsa en 'Sincronizar con Wall Street' para ver tus gráficos y datos.")

    # --- PESTAÑA CIERRES ANUALES ---
    with tabs[5]:
        st.subheader("🗓️ Histórico de Cierres y Rendimiento Anual")
        
        col_cl1, col_cl2 = st.columns([1, 3])
        
        with col_cl1:
            st.markdown("#### 🤝 Registrar Venta")
            with st.form("form_cierre"):
                tk_v = st.selectbox("Activo Vendido:", opciones_desplegable)
                prec_v = st.number_input("Rentabilidad Final (%):", format="%.2f")
                gan_v = st.number_input("Ganancia/Pérdida Efectiva (€/$):", format="%.2f")
                fecha_v = st.date_input("Fecha de Venta:")
                if st.form_submit_button("Registrar Cierre", use_container_width=True):
                    ws_cierres = conectar_ws("Cierres")
                    if ws_cierres:
                        t_v_limpio = tk_v.split(" ")[0]
                        ws_cierres.append_row([t_v_limpio, tickers_nombres.get(t_v_limpio, t_v_limpio), float(prec_v), float(gan_v), str(fecha_v)])
                        st.success("✅ Cierre registrado.")
                        time.sleep(1)
                        st.rerun()

            st.markdown("#### 📥 Cargar Datos")
            if st.button("Cargar Histórico de Cierres", use_container_width=True):
                ws_cierres = conectar_ws("Cierres")
                if ws_cierres:
                    try:
                        datos_cierres = ws_cierres.get_all_records()
                        if datos_cierres:
                            st.session_state.datos_cierres = datos_cierres
                        else:
                            st.session_state.datos_cierres = []
                            st.warning("No hay operaciones cerradas registradas.")
                    except Exception as e:
                        st.error(f"Error al leer la hoja 'Cierres': {e}")

        with col_cl2:
            if st.session_state.get('datos_cierres') and len(st.session_state.datos_cierres) > 0:
                df_cierres = pd.DataFrame(st.session_state.datos_cierres)
                
                # Nombres de columnas estrictos como los tienes en el Excel:
                col_rent = 'Rentabilidad_Final'
                col_gan = 'Ganancia_Efectiva'
                
                if col_gan in df_cierres.columns and col_rent in df_cierres.columns:
                    try:
                        df_cierres[col_gan] = df_cierres[col_gan].astype(str).str.replace(',', '.').astype(float)
                        df_cierres[col_rent] = df_cierres[col_rent].astype(str).str.replace(',', '.').astype(float)
                        
                        win_rate = (df_cierres[col_gan] > 0).mean() * 100
                        beneficio_neto = df_cierres[col_gan].sum()
                        operaciones_totales = len(df_cierres)
                        operaciones_ganadoras = len(df_cierres[df_cierres[col_gan] > 0])
                        operaciones_perdedoras = operaciones_totales - operaciones_ganadoras
                        
                        # --- KPIS CIERRES ---
                        c_b1, c_b2, c_b3, c_b4 = st.columns(4)
                        c_b1.metric("💸 Beneficio Neto Realizado", f"{beneficio_neto:+,.2f} €")
                        c_b2.metric("🎯 Win Rate", f"{win_rate:.1f}%")
                        c_b3.metric("🏆 Trades Ganadores", f"{operaciones_ganadoras}")
                        c_b4.metric("🪦 Trades Perdedores", f"{operaciones_perdedoras}")
                        
                        st.markdown("---")
                        
                        c_gc1, c_gc2 = st.columns(2)
                        with c_gc1:
                            if operaciones_totales > 0:
                                df_win_loss = pd.DataFrame({'Resultado': ['Ganadoras', 'Perdedoras'], 'Cantidad': [operaciones_ganadoras, operaciones_perdedoras]})
                                fig_win = px.pie(df_win_loss, values='Cantidad', names='Resultado', title='Tasa de Acierto (Win Rate)', hole=0.4, color='Resultado', color_discrete_map={'Ganadoras':'#228B22', 'Perdedoras':'#FF3333'})
                                st.plotly_chart(fig_win, use_container_width=True)
                            
                        with c_gc2:
                            df_cierres['Color'] = np.where(df_cierres[col_gan] > 0, 'green', 'red')
                            df_cierres['Operacion'] = [f"Op {i+1} ({tk})" for i, tk in enumerate(df_cierres.get('Ticker', df_cierres.index))]
                            fig_hist = px.bar(df_cierres, x='Operacion', y=col_gan, title='Historial de Trades Cerrados', color='Color', color_discrete_map={'green':'#228B22', 'red':'#FF3333'})
                            fig_hist.update_layout(showlegend=False, xaxis_title="", yaxis_title="Ganancia/Pérdida Efectiva")
                            st.plotly_chart(fig_hist, use_container_width=True)
                        
                        st.markdown("#### 📜 Registro de Operaciones Cerradas")
                        df_cierres_mostrar = df_cierres.copy()
                        df_cierres_mostrar[col_gan] = df_cierres_mostrar[col_gan].apply(lambda x: f"{x:+,.2f} €")
                        df_cierres_mostrar[col_rent] = df_cierres_mostrar[col_rent].apply(lambda x: f"{x:+.2f}%")
                        
                        cols_mostrar = ['Ticker', 'Empresa', col_rent, col_gan, 'Fecha_Venta']
                        cols_mostrar = [c for c in cols_mostrar if c in df_cierres_mostrar.columns]
                        
                        st.dataframe(df_cierres_mostrar[cols_mostrar].style.map(color_pct, subset=[col_gan, col_rent]), use_container_width=True, hide_index=True)
                    except Exception as e_proc:
                        st.error(f"Error procesando los números. Detalle: {e_proc}")
                else:
                    st.error("⚠️ No encuentro las columnas 'Ganancia_Efectiva' o 'Rentabilidad_Final'. Revisa tu Google Sheets.")
            else:
                st.info("👈 Pulsa en 'Cargar Histórico de Cierres' para ver tu rendimiento consolidado.")
            else:
                st.info("👈 Pulsa en 'Cargar Histórico de Cierres' para ver tu rendimiento consolidado.")
