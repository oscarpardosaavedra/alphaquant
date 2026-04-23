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

# ==========================================
# 1. CONFIGURACIÓN Y CONEXIÓN DB (BASE DE DATOS)
# ==========================================
st.set_page_config(page_title="Alphaquant", page_icon="📈", layout="wide")

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

# ==========================================
# 4. CABECERA Y SEMÁFOROS
# ==========================================
us, eu, asia = obtener_estado_mercados()
col1, col2, col3 = st.columns(3)
col1.info(f"**EE.UU:** {us['estado']} | Hora: {us['horario']} (Madrid)")
col2.info(f"**Europa:** {eu['estado']} | Hora: {eu['horario']} (Madrid)")
col3.info(f"**Asia:** {asia['estado']} | Hora: {asia['horario']} (Madrid)")
st.markdown("---")

tab1, tab2, tab3 = st.tabs(["🔬 Análisis Individual", "🎯 Cazar Alpha (Radar)", "🏆 Sala de Trofeos"])

# ------------------------------------------
# PESTAÑA 1: VISOR DE GRÁFICOS
# ------------------------------------------
with tab1:
    st.markdown("### 🔍 Selector de Activos")
    
    col_buscador, col_espacio = st.columns([1, 3])
    with col_buscador:
        ticker_elegido = st.selectbox("Elige la empresa que quieres revisar:", opciones_desplegable)
    
    if ticker_elegido:
        simbolo_real = ticker_elegido.split(" ")[0]
        simbolo_yahoo = a_yahoo(simbolo_real)
        
        st.markdown("---")
        
        periodo = st.radio(
            "Rango de tiempo:", 
            ["1 Mes", "3 Meses", "6 Meses", "1 Año", "5 Años", "10 Años", "Máximo"], 
            index=1, 
            horizontal=True
        )
        
        mapa_tiempo = {
            "1 Mes": "1mo", "3 Meses": "3mo", "6 Meses": "6mo", 
            "1 Año": "1y", "5 Años": "5y", "10 Años": "10y", "Máximo": "max"
        }
        
        with st.spinner(f"Cargando datos en vivo de {simbolo_real}..."):
            try:
                ticker_obj = yf.Ticker(simbolo_yahoo)
                datos = ticker_obj.history(period=mapa_tiempo[periodo])
                
                # ESCUDO ANTI-MULTIINDEX YAHOO FINANCE
                if isinstance(datos.columns, pd.MultiIndex):
                    datos.columns = datos.columns.get_level_values(0)
                
                if 'Close' in datos.columns and not datos.empty:
                    info = ticker_obj.info
                    moneda_codigo = info.get('currency', 'USD')
                    
                    datos_limpios = datos.dropna(subset=['Close'])
                    array_precios = datos_limpios['Close'].values.flatten()
                    precio_actual = float(array_precios[-1])
                    
                    simbolos_moneda = {"USD": "$", "EUR": "€", "GBP": "£", "GBp": "GBp", "JPY": "¥"}
                    s_moneda = simbolos_moneda.get(moneda_codigo, moneda_codigo)
                    
                    st.metric(label=f"Valor Actual ({simbolo_real})", value=f"{precio_actual:.2f} {s_moneda}")
                    
                    sector = info.get('sector', '')
                    industria = info.get('industry', '')
                    resumen_largo = info.get('longBusinessSummary', '')
                    
                    if sector and industria: 
                        st.caption(f"🏢 **Sector:** {sector} | **Industria:** {industria}")
                    if resumen_largo:
                        frases = resumen_largo.split('. ')
                        st.markdown(f"*{'. '.join(frases[:2]) + '.' if len(frases) > 2 else resumen_largo}*")
                    
                    fig = go.Figure()
                    x_data = datos_limpios.index
                    if isinstance(x_data, pd.MultiIndex):
                        x_data = x_data.get_level_values(0)
                    
                    fig.add_trace(go.Scatter(x=x_data, y=array_precios, mode='lines', name='Precio', line=dict(color='#228B22', width=2)))
                    fig.update_layout(title=f"Cotización: {ticker_elegido}", template='plotly_dark', margin=dict(l=0, r=0, t=40, b=0), xaxis_title="", yaxis_title=f"Precio ({s_moneda})", hovermode="x unified")
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("⚠️ Sin datos históricos disponibles para este periodo.")
            except Exception as e:
                st.error(f"⚠️ Error al cargar la gráfica. Detalle: {e}")

# ------------------------------------------
# PESTAÑA 2: RADAR DE CAZA CON AUTO-GUARDADO
# ------------------------------------------
with tab2:
    st.markdown("### 🎯 Selecciona tu Objetivo")
    
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
        if mercado_objetivo == "EEUU" and "Cerrado" in us["estado"]:
            st.warning("⚠️ **Aviso:** Wall Street está cerrado ahora mismo. Los datos corresponden al último cierre.")
        elif mercado_objetivo == "Europa" and "Cerrado" in eu["estado"]:
            st.warning("⚠️ **Aviso:** El mercado europeo está cerrado. Los datos corresponden al último cierre.")
        elif mercado_objetivo == "Asia" and "Cerrado" in asia["estado"]:
            st.warning("⚠️ **Aviso:** El mercado asiático está cerrado. Los datos corresponden al último cierre.")
        elif mercado_objetivo == "Todos" and "Cerrado" in us["estado"] and "Cerrado" in eu["estado"] and "Cerrado" in asia["estado"]:
            st.warning("⚠️ **Aviso:** Todos los mercados globales están cerrados en este momento.")

        tickers_a_escanear = [t for t in tickers_nombres.keys() if mercado_objetivo == "Todos" or obtener_region(t) == mercado_objetivo]
        
        st.info(f"Iniciando escaneo del algoritmo para: **{mercado_objetivo}** ({len(tickers_a_escanear)} activos)...")
        
        barra_progreso = st.progress(0, text="Conectando con Wall Street y calculando benchmark (SPY)...")
        resultados_radar = []
        
        alphaSPY = 0
        try:
            spy_hist = yf.Ticker("SPY").history(period="1mo")
            if isinstance(spy_hist.columns, pd.MultiIndex):
                spy_hist.columns = spy_hist.columns.get_level_values(0)
            if 'Close' in spy_hist.columns and len(spy_hist) >= 21:
                alphaSPY = ((float(spy_hist['Close'].iloc[-1]) / float(spy_hist['Close'].iloc[-21])) - 1) * 100
        except Exception: pass

        ws = conectar_db()
        existentes_en_db = []
        if ws:
            try:
                existentes_en_db = ws.col_values(1)
            except Exception:
                pass

        for i, ticker in enumerate(tickers_a_escanear):
            barra_progreso.progress((i + 1) / len(tickers_a_escanear), text=f"Evaluando: {ticker}...")
            try:
                sym_yahoo = a_yahoo(ticker)
                stock = yf.Ticker(sym_yahoo)
                
                hist_full = stock.history(period="max")
                if isinstance(hist_full.columns, pd.MultiIndex):
                    hist_full.columns = hist_full.columns.get_level_values(0)
                    
                if 'Close' not in hist_full.columns or 'Volume' not in hist_full.columns:
                    continue
                    
                hist_full = hist_full.dropna(subset=['Close'])
                if hist_full.empty or len(hist_full) < 2: continue
                
                array_cierres = hist_full['Close'].values.flatten()
                array_vol = hist_full['Volume'].values.flatten()
                
                precio_actual = float(array_cierres[-1])
                precio_ayer = float(array_cierres[-2])
                pct_hoy = ((precio_actual / precio_ayer) - 1) * 100 if precio_ayer > 0 else 0
                
                hist_1y = hist_full.iloc[-252:] if len(hist_full) >= 252 else hist_full
                max_52 = float(hist_1y['High'].max())
                min_52 = float(hist_1y['Low'].min())
                
                dist_suelo = ((precio_actual / min_52) - 1) * 100 if min_52 > 0 else 0
                dist_max = ((precio_actual / max_52) - 1) * 100 if max_52 > 0 else 0
                
                def get_ret(days):
                    if len(array_cierres) >= days and array_cierres[-days] > 0:
                        return ((precio_actual / float(array_cierres[-days])) - 1) * 100
                    return None

                r1m = get_ret(21)
                r6m = get_ret(126)
                r1y = get_ret(252)
                r5y = get_ret(1260)
                r10y = get_ret(2520)
                r20y = get_ret(5040)
                
                start_price = float(array_cierres[0])
                ret_max = ((precio_actual / start_price) - 1) * 100 if start_price > 0 else 0
                
                try:
                    info = stock.info
                    per = info.get('trailingPE', 999)
                    if per is None: per = 999 
                except:
                    info = {}
                    per = 999
                
                vol_hoy = float(array_vol[-1])
                vol_medio = float(np.mean(array_vol[-20:])) if len(array_vol) >= 20 else float(np.mean(array_vol))
                
                ptsBase = 40
                v_1m = r1m if r1m is not None else 0
                v_6m = r6m if r6m is not None else 0
                reg = obtener_region(ticker)
                
                if v_1m > 0 and v_6m > 0: ptsBase += 15
                elif v_1m > 0 and v_6m < -15: ptsBase -= 20
                elif v_6m <= 0: ptsBase -= 15
                else: ptsBase += 5
                
                myAlpha = v_1m - (alphaSPY if reg == "EEUU" else 0)
                if myAlpha > 5: ptsBase += 10
                elif myAlpha < -5: ptsBase -= 10
                
                isHyperGrowth = (myAlpha > 10 and vol_medio > 1000000)
                
                if reg == "EEUU":
                    if 0 < per <= 45: ptsBase += 15
                    elif 45 < per <= 120 and isHyperGrowth: ptsBase += 15
                    elif per > 120 or per < 0: ptsBase -= 15
                elif reg == "Europa":
                    if 0 < per <= 15: ptsBase += 15
                    elif 15 < per <= 35 and isHyperGrowth: ptsBase += 15
                    elif per > 35 or per < 0: ptsBase -= 15
                elif reg == "Asia":
                    if 0 < per <= 30: ptsBase += 15
                    elif 30 < per <= 80 and isHyperGrowth: ptsBase += 15
                    elif per > 80 or per < 0: ptsBase -= 15
                
                if abs(pct_hoy) > 4 and vol_hoy < vol_medio: ptsBase -= 15 
                
                if reg == "EEUU" or reg == "Europa":
                    if abs(pct_hoy) <= 1.5 and vol_hoy >= (vol_medio * 1.5): ptsBase += 20
                    elif dist_max > -5 and vol_hoy >= (vol_medio * 2.0) and pct_hoy > 2: ptsBase += 25
                    elif dist_max > -2 and vol_hoy > (vol_medio * 1.5) and pct_hoy > 0: ptsBase += 15
                    elif pct_hoy < -3 and vol_hoy > (vol_medio * 1.5): ptsBase -= 20
                elif reg == "Asia":
                    if abs(pct_hoy) <= 2.0 and vol_hoy >= (vol_medio * 2.0): ptsBase += 20
                    elif dist_max > -10 and vol_hoy >= (vol_medio * 2.5) and pct_hoy > 3: ptsBase += 25
                    elif dist_max > -5 and vol_hoy > (vol_medio * 1.8) and pct_hoy > 0: ptsBase += 15
                
                isFenix = False
                if dist_max <= -20 and per > 0 and vol_medio > 400000 and v_1m > 2 and pct_hoy > 1.5:
                    fuerzaGiro = (15 if v_1m > 8 else 5) + (15 if vol_hoy > vol_medio * 1.2 else 0) + (10 if pct_hoy > 2 else 5)
                    scoreFenix = 65 + fuerzaGiro
                    if scoreFenix > ptsBase: 
                        ptsBase = scoreFenix
                        isFenix = True
                
                pts = max(0, min(100, int(ptsBase)))

                if pts >= 90 and ticker not in existentes_en_db and ws is not None:
                    fecha_hoy = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                    nombre_empresa = tickers_nombres[ticker]
                    ws.append_row([ticker, nombre_empresa, fecha_hoy, float(precio_actual), int(pts)])
                    existentes_en_db.append(ticker)

                recomendacion = "❌ Esperar"
                if pts >= 65:
                    if pts >= 85: recomendacion = "🔥 COMPRA (FÉNIX ORO)" if isFenix else "🚀 COMPRA INSTITUCIONAL"
                    elif pts >= 70: recomendacion = "👀 VIGILAR (FÉNIX)" if isFenix else "💎 VIGILAR (BREAKOUT/WHALE)"
                
                moneda = info.get('currency', 'USD')
                simbolos_moneda = {"USD": "$", "EUR": "€", "GBP": "£", "GBp": "GBp", "JPY": "¥"}
                s_mon = simbolos_moneda.get(moneda, moneda)
                sector = info.get('sector', 'N/A')

                def fmt_pct(val): return f"{val:+.2f}%" if val is not None else "N/A"

                resultados_radar.append({
                    "TICKER": ticker, "NOMBRE": tickers_nombres[ticker], "PUNTOS": pts, "RECOMENDACIÓN": recomendacion,
                    "PRECIO": f"{precio_actual:.2f} {s_mon}", "% HOY": fmt_pct(pct_hoy), "% 1 MES": fmt_pct(r1m),
                    "% 6 MESES": fmt_pct(r6m), "% 1 AÑO": fmt_pct(r1y), "% 5 AÑOS": fmt_pct(r5y),
                    "% 10 AÑOS": fmt_pct(r10y), "% 20 AÑOS": fmt_pct(r20y), "% MÁX": fmt_pct(ret_max),
                    "PER": f"{per:.1f}" if per != 999 else "N/A", "SECTOR": sector,
                    "VOLUMEN": f"{vol_hoy:,.0f}", "VOL. MEDIO": f"{vol_medio:,.0f}",
                    "SUELO (52s)": fmt_pct(dist_suelo), "MAX (52s)": fmt_pct(dist_max)
                })
                
                time.sleep(0.3)
                
            except Exception as e: 
                time.sleep(0.3)
                continue
            
        barra_progreso.progress(100, text="✅ 100% Completado")
        
        if resultados_radar:
            df = pd.DataFrame(resultados_radar)
            df = df.sort_values(by="PUNTOS", ascending=False).reset_index(drop=True)
            
            def color_porcentajes(val):
                if isinstance(val, str) and '%' in val:
                    if val.startswith('+'): return 'color: #228B22;' 
                    elif val.startswith('-'): return 'color: #FF3333;' 
                return ''

            def negrita_ticker(val): return 'font-weight: bold;' 

            columnas_pct = ["% HOY", "% 1 MES", "% 6 MESES", "% 1 AÑO", "% 5 AÑOS", "% 10 AÑOS", "% 20 AÑOS", "% MÁX", "SUELO (52s)", "MAX (52s)"]
            
            try:
                styled_df = df.style.map(color_porcentajes, subset=columnas_pct).map(negrita_ticker, subset=['TICKER'])
            except AttributeError:
                styled_df = df.style.applymap(color_porcentajes, subset=columnas_pct).applymap(negrita_ticker, subset=['TICKER'])

            st.success("Caza terminada. Las empresas con 90 puntos o más se han guardado automáticamente en la base de datos.")
            
            st.dataframe(
                styled_df, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "PUNTOS": st.column_config.NumberColumn(help="Puntuación del algoritmo: Evalúa Alpha relativo, fundamentales y volumen según la región."),
                    "RECOMENDACIÓN": st.column_config.TextColumn(help="Requiere mínimo de 65 puntos para dar señal."),
                    "% HOY": st.column_config.TextColumn(help="Variación en la sesión actual."),
                    "% 1 MES": st.column_config.TextColumn(help="Rendimiento en 21 días laborables."),
                    "% 6 MESES": st.column_config.TextColumn(help="Rendimiento en 126 días laborables."),
                    "% 1 AÑO": st.column_config.TextColumn(help="Rendimiento en 252 sesiones."),
                    "% 5 AÑOS": st.column_config.TextColumn(help="Rendimiento a medio plazo (1260 sesiones)."),
                    "% 10 AÑOS": st.column_config.TextColumn(help="Rendimiento a largo plazo (2520 sesiones)."),
                    "% 20 AÑOS": st.column_config.TextColumn(help="Rendimiento a súper largo plazo (5040 sesiones)."),
                    "% MÁX": st.column_config.TextColumn(help="Rendimiento histórico total."),
                    "PER": st.column_config.TextColumn(help="Price-to-Earnings. El algoritmo pondera los PER altos si hay hipercrecimiento, ajustado por mercado."),
                    "SECTOR": st.column_config.TextColumn(help="Sector económico."),
                    "VOLUMEN": st.column_config.TextColumn(help="Volumen de la última sesión."),
                    "VOL. MEDIO": st.column_config.TextColumn(help="Media diaria de acciones negociadas (20d)."),
                    "SUELO (52s)": st.column_config.TextColumn(help="Distancia al precio MÍNIMO del último año."),
                    "MAX (52s)": st.column_config.TextColumn(help="Distancia al precio MÁXIMO del último año.")
                }
            )
        else:
            st.error("No se han podido descargar datos en este momento. Yahoo Finance podría estar limitando temporalmente tus peticiones.")


# ------------------------------------------
# PESTAÑA 3: SALA DE TROFEOS (DB REAL Y BORRADO MANUAL)
# ------------------------------------------
with tab3:
    st.markdown("### 🏆 Sala de Trofeos")
    st.write("Verifica en tiempo real el porcentaje de acierto. Las acciones que alcanzan o superan los 90 puntos en el Radar se guardan aquí de forma permanente.")
    
    ws = conectar_db()
    
    if ws is not None:
        data_sheet = ws.get_all_records()
        
        if not data_sheet:
            st.info("Tu base de datos está vacía. Ve a la pestaña de Radar y haz un escaneo para cazar nuevas acciones.")
        else:
            with st.expander("🗑️ Gestionar Base de Datos (Eliminar Tickers)"):
                st.write("Si alguna acción ya no te interesa, puedes borrarla desde aquí:")
                with st.form("form_del"):
                    tk_borrar = st.selectbox("Selecciona el Ticker a eliminar:", [d['Ticker'] for d in data_sheet])
                    
                    if st.form_submit_button("Borrar permanentemente"):
                        cell = ws.find(tk_borrar, in_column=1)
                        if cell:
                            ws.delete_rows(cell.row)
                            st.success(f"✅ El ticker {tk_borrar} se ha eliminado de la base de datos.")
                            time.sleep(1.5) 
                            st.rerun() 
                        else:
                            st.error("No se ha encontrado el ticker en la base de datos.")

            if st.button("🔄 Auditar Rendimiento Actual", use_container_width=True):
                with st.spinner("Sincronizando Wall Street y calculando métricas avanzadas..."):
                    pendiente = []
                    exitos = []
                    cuarentena = []
                    fracasos = []
                    alpha_total = 0
                    
                    for d in data_sheet:
                        try:
                            tk_y = a_yahoo(d['Ticker'])
                            tk = yf.Ticker(tk_y)
                            
                            hist = tk.history(period="1y")
                            if isinstance(hist.columns, pd.MultiIndex):
                                hist.columns = hist.columns.get_level_values(0)
                                
                            if 'Close' not in hist.columns or hist.empty: continue
                            
                            hist = hist.dropna(subset=['Close'])
                            array_cierres = hist['Close'].values.flatten()
                            
                            p_hoy = float(array_cierres[-1])
                            p_entrada = float(str(d['Precio_Aviso']).replace(',', '.'))
                            fecha_str = str(d['Fecha'])
                            
                            rent = ((p_hoy / p_entrada) - 1) * 100
                            
                            rent_max = rent
                            ignicion = "N/A"
                            try:
                                fecha_compra_date = pd.to_datetime(fecha_str).tz_localize(None).date()
                                hist_index_tz_naive = hist.index.tz_localize(None)
                                mask = hist_index_tz_naive.date >= fecha_compra_date
                                hist_post = hist[mask]
                            except:
                                hist_post = hist
                                
                            if not hist_post.empty:
                                high_series = hist_post['High']
                                max_p_real = float(high_series.max())
                                rent_max = ((max_p_real / p_entrada) - 1) * 100
                                
                                hit_5 = hist_post[high_series >= p_entrada * 1.05]
                                if not hit_5.empty:
                                    dias_ign = (hit_5.index[0].date() - hist_post.index[0].date()).days
                                    ignicion = f"{dias_ign}d"
                            
                            try:
                                info = tk.info
                                moneda = info.get('currency', 'USD')
                            except:
                                moneda = "USD"
                            
                            simbolos_moneda = {"USD": "$", "EUR": "€", "GBP": "£", "GBp": "GBp", "JPY": "¥"}
                            s_mon = simbolos_moneda.get(moneda, moneda)
                            
                            reg = obtener_region(d['Ticker'])
                            bandera = "🇺🇸" if reg == "EEUU" else ("🇪🇺" if reg == "Europa" else "⛩️")

                            if abs(rent) < 0.01:
                                motivo = "A la espera de apertura o movimiento de mercado."
                            elif rent > 0: 
                                motivo = "Tendencia confirmada con entrada de capital institucional."
                            elif rent >= -3.0: 
                                motivo = "Ruido normal de mercado. Consolidando el soporte."
                            else: 
                                motivo = "Ruptura de soporte. Caída severa fuera de control."

                            obj = {
                                "T": d['Ticker'], "N": d['Empresa'], "E": p_entrada, 
                                "A": p_hoy, "R": rent, "F": fecha_str, "S_MON": s_mon,
                                "RMAX": rent_max, "IGN": ignicion, "B": bandera, "M": motivo
                            }
                            
                            alpha_total += rent
                            
                            if abs(rent) < 0.01: pendiente.append(obj)
                            elif rent > 0: exitos.append(obj)
                            elif rent >= -3.0: cuarentena.append(obj)
                            else: fracasos.append(obj)
                                
                        except Exception as e: 
                            continue
                    
                    tot = len(pendiente) + len(exitos) + len(cuarentena) + len(fracasos)
                    
                    if tot > 0:
                        tot_abiertas = tot - len(pendiente)
                        win_rate = (len(exitos) / tot_abiertas) * 100 if tot_abiertas > 0 else 0
                        alpha_medio = alpha_total / tot
                        
                        st.markdown("---")
                        m1, m2, m3 = st.columns(3)
                        m1.metric(label="🎯 Precisión (Win Rate)", value=f"{win_rate:.1f}%")
                        m2.metric(label="⚔️ Alpha Medio", value=f"{alpha_medio:+.2f}%", delta=f"{alpha_medio:+.2f}%")
                        m3.metric(label="⏱️ Base de Datos", value=f"{tot} Activos")
                        st.markdown("---")
                        
                        c_p, c_w, c_q, c_l = st.columns(4)
                        
                        def pintar_tarjeta(item, color_borde, color_texto):
                            ign_html = f'<span class="stat-badge" title="Días hasta tocar un +5% (Ignición)">Ign: {item["IGN"]}</span>' if item['IGN'] != "N/A" else ""
                            return f"""<div style="background: white; border-radius: 6px; padding: 10px; margin-bottom: 8px; border-top: 3px solid {color_borde}; box-shadow: 0 2px 4px rgba(0,0,0,0.08);"><div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 5px;"><div style="display: flex; align-items: baseline; gap: 6px;"><span title="{item['N']}" style="font-weight: 900; color: #073763; font-size: 14px;">{item['B']} {item['T']}</span><span style="color: #95a5a6; font-size: 10px;">Fecha: {item['F']}</span></div><div style="font-weight: 900; font-size: 14px; color: {color_texto};">{item['R']:+.2f}%</div></div><div style="font-size: 11px; color: #555; margin-bottom: 5px;">In: <b>{item['E']:.2f}{item['S_MON']}</b> &rarr; Hoy: <b>{item['A']:.2f}{item['S_MON']}</b></div><div style="display: flex; gap: 5px; margin-bottom: 6px;"><span class="stat-badge" title="Rentabilidad Máxima Histórica">Max: {item['RMAX']:+.1f}%</span>{ign_html}</div><div style="font-size: 10px; color: #7f8c8d; font-style: italic; background: #f9fbfd; padding: 4px; border-radius: 4px;">Nota: {item['M']}</div></div>"""

                        with c_p:
                            st.markdown('<h4 style="margin-bottom:15px; font-size: 16px; color:#34495e;" title="Acciones a 0.00%. Acaban de ser escaneadas o su mercado está cerrado.">⏸️ Pendiente</h4>', unsafe_allow_html=True)
                            if pendiente:
                                for p in sorted(pendiente, key=lambda x: x["T"]):
                                    st.markdown(pintar_tarjeta(p, "#bdc3c7", "#7f8c8d"), unsafe_allow_html=True)
                            else:
                                st.info("Nada pendiente.")

                        with c_w:
                            st.markdown('<h4 style="margin-bottom:15px; font-size: 16px; color:#228B22;" title="Activos en ganancias (> 0%). El algoritmo ha acertado.">🏆 Éxitos</h4>', unsafe_allow_html=True)
                            if exitos:
                                for e in sorted(exitos, key=lambda x: x["R"], reverse=True):
                                    st.markdown(pintar_tarjeta(e, "#228B22", "#228B22"), unsafe_allow_html=True)
                            else:
                                st.info("Aún no hay éxitos.")
                                
                        with c_q:
                            st.markdown('<h4 style="margin-bottom:15px; font-size: 16px; color:#f39c12;" title="Pérdida menor al 3%. Se considera ruido normal de mercado o coste del spread.">⏳ Cuarentena</h4>', unsafe_allow_html=True)
                            if cuarentena:
                                for q in sorted(cuarentena, key=lambda x: x["R"], reverse=True):
                                    st.markdown(pintar_tarjeta(q, "#f39c12", "#f39c12"), unsafe_allow_html=True)
                            else:
                                st.info("Sin activos consolidando.")
                                
                        with c_l:
                            st.markdown('<h4 style="margin-bottom:15px; font-size: 16px; color:#FF3333;" title="Caída superior al 3%. Ruptura de soporte, posible fallo de señal.">🪦 Cementerio</h4>', unsafe_allow_html=True)
                            if fracasos:
                                for f in sorted(fracasos, key=lambda x: x["R"]):
                                    st.markdown(pintar_tarjeta(f, "#FF3333", "#FF3333"), unsafe_allow_html=True)
                            else:
                                st.info("No hay fallos registrados.")

                    else:
                        st.warning("⚠️ No se han podido auditar los datos de rendimiento.")
