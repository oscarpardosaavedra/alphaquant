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
# 1. CONFIGURACIÓN Y CONEXIÓN CON BASE DE DATOS
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
        st.error("⚠️ Error de conexión con la base de datos. Verifique la configuración de acceso.")
        return None

# ==========================================
# 2. ESTILOS CSS (DISEÑO TITANIUM)
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
    .trophy-card { 
        background-color: white; border-radius: 8px; padding: 15px 20px; 
        margin-bottom: 12px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); 
        border-left: 5px solid #228B22; display: flex; justify-content: space-between; align-items: center; 
    }
    .cemetery-card { 
        background-color: white; border-radius: 8px; padding: 15px 20px; 
        margin-bottom: 12px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); 
        border-left: 5px solid #FF3333; display: flex; justify-content: space-between; align-items: center; 
    }
    .card-left { text-align: left; }
    .card-right { text-align: right; }
    .card-title { margin: 0; font-size: 16px; color: #073763; font-weight: 900; }
    .card-subtitle { font-size: 13px; color: #7f8c8d; font-weight: normal; }
    .card-pct-win { margin: 0; font-size: 20px; color: #228B22; font-weight: 900; }
    .card-pct-lose { margin: 0; font-size: 20px; color: #FF3333; font-weight: 900; }
    .card-details { margin: 6px 0 0 0; font-size: 13px; color: #555; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div style="background: linear-gradient(135deg, #0f2027 0%, #203a43 50%, #2c5364 100%); padding: 25px; border-radius: 12px; text-align: center; margin-bottom: 30px; box-shadow: 0 4px 8px rgba(0,0,0,0.1);">
    <h1 style="color: white; margin: 0; font-size: 2.8em; font-family: 'Segoe UI', Tahoma, sans-serif; letter-spacing: 2px;">📈 ALPHAQUANT</h1>
</div>
""", unsafe_allow_html=True)

# ==========================================
# 3. DICCIONARIO DE ACTIVOS Y UTILIDADES
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
    
    hora_ny = ahora_utc.astimezone(pytz.timezone('US/Eastern'))
    t_ny = hora_ny.time()
    if hora_ny.weekday() >= 5: est_us = "🔴 Cerrado"
    elif datetime.time(4, 0) <= t_ny < datetime.time(9, 30): est_us = "🟡 Pre-Market"
    elif datetime.time(9, 30) <= t_ny < datetime.time(16, 0): est_us = "🟢 Abierto"
    elif datetime.time(16, 0) <= t_ny < datetime.time(20, 0): est_us = "🔵 Post-Market"
    else: est_us = "🔴 Cerrado"

    hora_eu = ahora_utc.astimezone(pytz.timezone('Europe/Madrid'))
    t_eu = hora_eu.time()
    if hora_eu.weekday() >= 5: est_eu = "🔴 Cerrado"
    elif datetime.time(9, 0) <= t_eu < datetime.time(17, 30): est_eu = "🟢 Abierto"
    else: est_eu = "🔴 Cerrado"

    hora_as = ahora_utc.astimezone(pytz.timezone('Asia/Tokyo'))
    if hora_as.weekday() >= 5: est_as = "🔴 Cerrado"
    elif datetime.time(9, 0) <= hora_as.time() <= datetime.time(15, 0): est_as = "🟢 Abierto"
    else: est_as = "🔴 Cerrado"
    
    return est_us, est_eu, est_as

# ==========================================
# 4. CABECERA Y SEMÁFOROS
# ==========================================
us, eu, asia = obtener_estado_mercados()
col1, col2, col3 = st.columns(3)
col1.info(f"**EEUU:** {us}")
col2.info(f"**Europa:** {eu}")
col3.info(f"**Asia:** {asia}")
st.markdown("---")

tab1, tab2, tab3 = st.tabs(["🔬 Análisis Individual", "🎯 Cazar Alpha (Radar)", "🏆 Sala de Trofeos"])

# ------------------------------------------
# PESTAÑA 1: VISOR DE GRÁFICOS
# ------------------------------------------
with tab1:
    st.markdown("### 🔍 Selector de Activos")
    
    col_buscador, col_espacio = st.columns([1, 3])
    with col_buscador:
        ticker_elegido = st.selectbox("Busque un activo en la base de datos:", opciones_desplegable)
    
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
        
        with st.spinner(f"Sincronizando datos de {simbolo_real}..."):
            try:
                ticker_obj = yf.Ticker(simbolo_yahoo)
                datos = ticker_obj.history(period=mapa_tiempo[periodo])
                
                if not datos.empty:
                    info = ticker_obj.info
                    moneda_codigo = info.get('currency', 'USD')
                    precio_actual = datos['Close'].iloc[-1]
                    
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
                        resumen_corto = '. '.join(frases[:2]) + '.' if len(frases) > 2 else resumen_largo
                        st.markdown(f"*{resumen_corto}*")
                    
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=datos.index, y=datos['Close'], mode='lines', line=dict(color='#228B22', width=2)))
                    fig.update_layout(template='plotly_dark', margin=dict(l=0, r=0, t=40, b=0), xaxis_title="", yaxis_title=f"Precio ({s_moneda})", hovermode="x unified")
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("⚠️ Sin datos históricos disponibles para este periodo.")
            except Exception:
                st.error("⚠️ Error en la descarga de datos bursátiles.")

# ------------------------------------------
# PESTAÑA 2: RADAR DE CAZA CON AUTO-GUARDADO
# ------------------------------------------
with tab2:
    st.markdown("### 🎯 Objetivo de Escaneo")
    
    c1, c2, c3, c4 = st.columns(4)
    btn_todos = c1.button("Ejecutar Escaneo Global", use_container_width=True)
    btn_us = c2.button("Mercado EE.UU.", use_container_width=True)
    btn_eu = c3.button("Mercado Europa", use_container_width=True)
    btn_asia = c4.button("Mercado Asia", use_container_width=True)
    
    mercado_objetivo = None
    if btn_todos: mercado_objetivo = "Todos"
    elif btn_us: mercado_objetivo = "EEUU"
    elif btn_eu: mercado_objetivo = "Europa"
    elif btn_asia: mercado_objetivo = "Asia"

    if mercado_objetivo:
        targets = [t for t in tickers_nombres.keys() if mercado_objetivo == "Todos" or obtener_region(t) == mercado_objetivo]
        st.info(f"Escaneando mercado: **{mercado_objetivo}** ({len(targets)} activos)...")
        
        barra_progreso = st.progress(0, text="Calculando benchmark...")
        resultados_radar = []
        
        alphaSPY = 0
        try:
            spy_hist = yf.Ticker("SPY").history(period="1mo")
            if len(spy_hist) >= 21:
                alphaSPY = ((spy_hist['Close'].iloc[-1] / spy_hist['Close'].iloc[-21]) - 1) * 100
        except Exception: pass

        ws = conectar_db()
        existentes_en_db = ws.col_values(1) if ws else []

        for i, ticker in enumerate(targets):
            barra_progreso.progress((i + 1) / len(targets), text=f"Evaluando: {ticker}...")
            
            try:
                sym_yahoo = a_yahoo(ticker)
                stock = yf.Ticker(sym_yahoo)
                hist_full = stock.history(period="max").dropna(subset=['Close'])
                if hist_full.empty or len(hist_full) < 2: continue
                
                p_act = float(hist_full['Close'].iloc[-1])
                p_prev = float(hist_full['Close'].iloc[-2])
                pct_hoy = ((p_act / p_prev) - 1) * 100 if p_prev > 0 else 0
                
                def get_ret(days):
                    if len(hist_full) >= days and hist_full['Close'].iloc[-days] > 0:
                        return ((p_act / hist_full['Close'].iloc[-days]) - 1) * 100
                    return None

                r1m, r6m, r1y = get_ret(21), get_r(126), get_r(252)
                r5y, r10y, r20y = get_r(1260), get_r(2520), get_r(5040)
                
                ret_max = ((p_act / hist_full['Close'].iloc[0]) - 1) * 100 if hist_full['Close'].iloc[0] > 0 else 0
                
                hist_1y = hist_full.iloc[-252:] if len(hist_full) >= 252 else hist_full
                max_52 = float(hist_1y['High'].max())
                min_52 = float(hist_1y['Low'].min())
                
                dist_suelo = ((p_act / min_52) - 1) * 100 if min_52 > 0 else 0
                dist_max = ((p_act / max_52) - 1) * 100 if max_52 > 0 else 0

                info = stock.info
                per = info.get('trailingPE', 999)
                vol_hoy = float(hist_full['Volume'].iloc[-1])
                vol_medio = float(hist_full['Volume'].tail(20).mean())
                
                # Lógica del Algoritmo
                pts = 50
                v_1m = r1m if r1m else 0
                v_6m = r6m if r6m else 0
                if v_1m > 0 and v_6m > 0: pts += 15
                elif v_6m <= 0: pts -= 15
                
                reg = obtener_region(ticker)
                myAlpha = v_1m - (alphaSPY if reg == "EEUU" else 0)
                if myAlpha > 5: pts += 10
                elif myAlpha < -5: pts -= 10
                
                isHyperGrowth = (myAlpha > 10 and vol_medio > 1000000)
                
                if reg == "EEUU":
                    if 0 < per <= 45: pts += 15
                    elif 45 < per <= 120 and isHyperGrowth: pts += 15
                    elif per > 120 or per < 0: pts -= 15
                elif reg == "Europa":
                    if 0 < per <= 15: pts += 15
                    elif 15 < per <= 35 and isHyperGrowth: pts += 15
                    elif per > 35 or per < 0: pts -= 15
                elif reg == "Asia":
                    if 0 < per <= 30: pts += 15
                    elif 30 < per <= 80 and isHyperGrowth: pts += 15
                    elif per > 80 or per < 0: pts -= 15
                
                if abs(pct_hoy) > 4 and vol_hoy < vol_medio: pts -= 15 
                
                if reg == "EEUU" or reg == "Europa":
                    if abs(pct_hoy) <= 1.5 and vol_hoy >= (vol_medio * 1.5): pts += 20
                    elif dist_max > -5 and vol_hoy >= (vol_medio * 2.0) and pct_hoy > 2: pts += 25
                    elif dist_max > -2 and vol_hoy > (vol_medio * 1.5) and pct_hoy > 0: pts += 15
                elif reg == "Asia":
                    if abs(pct_hoy) <= 2.0 and vol_hoy >= (vol_medio * 2.0): pts += 20
                    elif dist_max > -10 and vol_hoy >= (vol_medio * 2.5) and pct_hoy > 3: pts += 25
                    elif dist_max > -5 and vol_hoy > (vol_medio * 1.8) and pct_hoy > 0: pts += 15
                
                isFenix = False
                if dist_max <= -20 and per > 0 and vol_medio > 400000 and v_1m > 2 and pct_hoy > 1.5:
                    fuerzaGiro = (15 if v_1m > 8 else 5) + (15 if vol_hoy > vol_medio * 1.2 else 0) + (10 if pct_hoy > 2 else 5)
                    scoreFenix = 65 + fuerzaGiro
                    if scoreFenix > pts: 
                        pts = scoreFenix
                        isFenix = True
                
                pts = max(0, min(100, int(pts)))

                # Guardado automático (>= 90 PUNTOS)
                if pts >= 90 and ticker not in existentes_en_db and ws:
                    fecha_hoy = datetime.datetime.now().strftime("%Y-%m-%d")
                    ws.append_row([ticker, tickers_nombres[ticker], fecha_hoy, float(p_act), int(pts)])
                    existentes_en_db.append(ticker)

                recomendacion = "❌ Esperar"
                if pts >= 65:
                    if pts >= 85: recomendacion = "🔥 COMPRA (FÉNIX ORO)" if isFenix else "🚀 COMPRA INSTITUCIONAL"
                    elif pts >= 70: recomendacion = "👀 VIGILAR (FÉNIX)" if isFenix else "💎 VIGILAR (BREAKOUT/WHALE)"

                moneda = info.get('currency', 'USD')
                simbolos_moneda = {"USD": "$", "EUR": "€", "GBP": "£", "GBp": "GBp", "JPY": "¥"}
                s_mon = simbolos_moneda.get(moneda, moneda)

                def fmt_pct(val): return f"{val:+.2f}%" if val is not None else "N/A"

                resultados_radar.append({
                    "TICKER": ticker, "NOMBRE": tickers_nombres[ticker], "PUNTOS": pts, "RECOMENDACIÓN": recomendacion,
                    "PRECIO": f"{p_act:.2f} {s_mon}", "% HOY": fmt_pct(pct_hoy), "% 1 MES": fmt_pct(r1m),
                    "% 6 MESES": fmt_pct(r6m), "% 1 AÑO": fmt_pct(r1y), "% 5 AÑOS": fmt_pct(r5y),
                    "% 10 AÑOS": fmt_pct(r10y), "% 20 AÑOS": fmt_pct(r20y), "% MÁX": fmt_pct(ret_max),
                    "PER": f"{per:.1f}" if per != 999 else "N/A", "SECTOR": sector,
                    "VOLUMEN": f"{vol_hoy:,.0f}", "VOL. MEDIO": f"{vol_medio:,.0f}",
                    "SUELO (52s)": fmt_pct(dist_suelo), "MAX (52s)": fmt_pct(dist_max)
                })
            except Exception: continue
            
        barra_progreso.progress(100, text="✅ 100% Completado")
        
        if resultados_radar:
            df = pd.DataFrame(resultados_radar).sort_values("PUNTOS", ascending=False).reset_index(drop=True)
            def color_pct(val):
                if isinstance(val, str) and val.startswith('+'): return 'color: #228B22;'
                elif isinstance(val, str) and val.startswith('-'): return 'color: #FF3333;'
                return ''
            
            styled_df = df.style.map(color_pct, subset=["% HOY", "% 1 MES", "% 6 MESES", "% 1 AÑO", "% 5 AÑOS", "% 10 AÑOS", "% 20 AÑOS", "% MÁX", "SUELO (52s)", "MAX (52s)"])
            st.success("Análisis completado. Activos con puntuación superior a 90 registrados en base de datos.")
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
        else:
            st.error("No se han podido descargar datos en este momento.")

# ------------------------------------------
# PESTAÑA 3: SALA DE TROFEOS (GESTIÓN DB)
# ------------------------------------------
with tab3:
    st.markdown("### 🏆 Sala de Trofeos")
    st.write("Auditoría de rendimiento de los activos con mayor puntuación histórica.")
    
    ws = conectar_db()
    if ws:
        data_sheet = ws.get_all_records()
        if not data_sheet:
            st.info("La base de datos está vacía. Registre activos mediante el radar.")
        else:
            with st.expander("🗑️ Gestión de Registros"):
                tk_borrar = st.selectbox("Seleccione activo a eliminar:", [d['Ticker'] for d in data_sheet])
                if st.button("Eliminar permanentemente"):
                    cell = ws.find(tk_borrar, in_column=1)
                    if cell:
                        ws.delete_rows(cell.row)
                        st.success("Registro eliminado."); time.sleep(1); st.rerun()

            if st.button("🔄 Ejecutar Auditoría de Rendimiento", use_container_width=True):
                with st.spinner("Actualizando valoraciones..."):
                    exitos, fracasos, alpha_total = [], [], 0
                    for d in data_sheet:
                        try:
                            tk = yf.Ticker(a_yahoo(d['Ticker']))
                            hist_1d = tk.history(period="1d")
                            if hist_1d.empty: continue
                            p_hoy = hist_1d['Close'].iloc[-1]
                            
                            info = tk.info
                            moneda = info.get('currency', 'USD')
                            simbolos_moneda = {"USD": "$", "EUR": "€", "GBP": "£", "GBp": "GBp", "JPY": "¥"}
                            s_mon = simbolos_moneda.get(moneda, moneda)

                            rent = ((p_hoy / float(d['Precio_Aviso'])) - 1) * 100
                            obj = {
                                "T": d['Ticker'], "N": d['Empresa'], "E": float(d['Precio_Aviso']), 
                                "A": p_hoy, "R": rent, "F": d['Fecha'], "S_MON": s_mon
                            }
                            alpha_total += rent
                            if rent > 0: exitos.append(obj)
                            else: fracasos.append(obj)
                        except Exception: continue
                    
                    tot = len(exitos) + len(fracasos)
                    if tot > 0:
                        st.markdown("---")
                        m1, m2, m3 = st.columns(3)
                        m1.metric("Win Rate", f"{(len(exitos)/tot)*100:.1f}%", help="Tasa de éxito de las señales.")
                        m2.metric("Alpha Medio", f"{alpha_total/tot:+.2f}%", help="Rentabilidad media de la selección.")
                        m3.metric("Muestra", f"{tot} Activos", help="Tamaño de la base de datos auditada.")
                        st.markdown("---")
                        
                        c_w, c_l = st.columns(2)
                        with c_w:
                            st.markdown("#### 🏆 Éxitos")
                            for e in sorted(exitos, key=lambda x: x["R"], reverse=True):
                                st.markdown(f'<div class="trophy-card"><div class="card-left"><p class="card-title">{e["T"]} <span class="card-subtitle">({e["N"]})</span></p><p class="card-details">Entrada: {e["E"]:.2f} {e["S_MON"]} ({e["F"]}) ➔ Hoy: {e["A"]:.2f} {e["S_MON"]}</p></div><p class="card-pct-win">+{e["R"]:.2f}%</p></div>', unsafe_allow_html=True)
                        with c_l:
                            st.markdown("#### 🪦 Cementerio")
                            for f in sorted(fracasos, key=lambda x: x["R"]):
                                st.markdown(f'<div class="cemetery-card"><div class="card-left"><p class="card-title">{f["T"]} <span class="card-subtitle">({f["N"]})</span></p><p class="card-details">Entrada: {f["E"]:.2f} {f["S_MON"]} ({f["F"]}) ➔ Hoy: {f["A"]:.2f} {f["S_MON"]}</p></div><p class="card-pct-lose">{f["R"]:.2f}%</p></div>', unsafe_allow_html=True)

