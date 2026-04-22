import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import datetime
import pytz
import time

# ==========================================
# 1. CONFIGURACIÓN Y ESTILOS CSS (DISEÑO PREMIUM)
# ==========================================
st.set_page_config(page_title="Alphaquant Titanium", page_icon="📈", layout="wide")

# Inyección de CSS para forzar cabeceras en negrita y mejorar el diseño
st.markdown("""
<style>
    /* Forzar cabeceras de la tabla en negrita absoluta */
    [data-testid="stDataFrame"] th {
        font-weight: 900 !important;
        font-size: 14px !important;
    }
    
    /* Estilo para las tarjetas de métricas */
    [data-testid="stMetric"] {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
    }
</style>
""", unsafe_allow_html=True)

# Banner Superior Premium (Reemplaza al st.title básico)
st.markdown("""
<div style="background: linear-gradient(135deg, #0f2027 0%, #203a43 50%, #2c5364 100%); padding: 30px; border-radius: 12px; text-align: center; margin-bottom: 30px; box-shadow: 0 8px 16px rgba(0,0,0,0.2);">
    <h1 style="color: white; margin: 0; font-size: 3em; font-family: 'Segoe UI', Tahoma, sans-serif; letter-spacing: 2px;">📈 ALPHAQUANT <span style="color: #00FF41;">TITANIUM</span></h1>
    <p style="color: #a8b2b8; font-size: 1.2em; font-style: italic; margin-top: 10px; letter-spacing: 1px;">Motor Cuantitativo Institucional V4 | Multi-Mercado</p>
</div>
""", unsafe_allow_html=True)

# Base de datos de Tickers
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

# ==========================================
# 2. MOTOR DE RELOJES Y SEMÁFOROS
# ==========================================
def obtener_estado_mercados():
    ahora_utc = datetime.datetime.now(pytz.utc)
    
    hora_ny = ahora_utc.astimezone(pytz.timezone('US/Eastern'))
    es_fin_semana_ny = hora_ny.weekday() >= 5
    t_ny = hora_ny.time()
    if es_fin_semana_ny: estado_us = "🔴 Cerrado"
    elif datetime.time(4, 0) <= t_ny < datetime.time(9, 30): estado_us = "🟡 Pre-Market"
    elif datetime.time(9, 30) <= t_ny < datetime.time(16, 0): estado_us = "🟢 Abierto"
    elif datetime.time(16, 0) <= t_ny < datetime.time(20, 0): estado_us = "🔵 Post-Market"
    else: estado_us = "🔴 Cerrado"

    hora_eu = ahora_utc.astimezone(pytz.timezone('Europe/Madrid'))
    es_fin_semana_eu = hora_eu.weekday() >= 5
    t_eu = hora_eu.time()
    if es_fin_semana_eu: estado_eu = "🔴 Cerrado"
    elif datetime.time(9, 0) <= t_eu < datetime.time(17, 30): estado_eu = "🟢 Abierto"
    else: estado_eu = "🔴 Cerrado"

    hora_asia = ahora_utc.astimezone(pytz.timezone('Asia/Tokyo'))
    es_fin_semana_asia = hora_asia.weekday() >= 5
    t_asia = hora_asia.time()
    if es_fin_semana_asia: estado_asia = "🔴 Cerrado"
    elif datetime.time(9, 0) <= t_asia <= datetime.time(15, 0): estado_asia = "🟢 Abierto"
    else: estado_asia = "🔴 Cerrado"
    
    return estado_us, estado_eu, estado_asia

st.markdown("### 🚦 Estado Global de los Mercados")
us, eu, asia = obtener_estado_mercados()
col1, col2, col3 = st.columns(3)
col1.info(f"**EEUU:** {us}")
col2.info(f"**Europa:** {eu}")
col3.info(f"**Asia:** {asia}")
st.markdown("---")

# ==========================================
# 4. PESTAÑAS PRINCIPALES 
# ==========================================
tab1, tab2 = st.tabs(["🔬 Análisis Individual", "🎯 Cazar Alpha (Radar)"])

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
            ["1 Mes", "3 Meses", "6 Meses", "1 Año", "Máximo"], 
            index=1, 
            horizontal=True
        )
        
        mapa_tiempo = {"1 Mes": "1mo", "3 Meses": "3mo", "6 Meses": "6mo", "1 Año": "1y", "Máximo": "max"}
        
        with st.spinner(f"Cargando datos en vivo de {simbolo_real}..."):
            try:
                ticker_obj = yf.Ticker(simbolo_yahoo)
                datos = ticker_obj.history(period=mapa_tiempo[periodo])
                
                if not datos.empty:
                    info = ticker_obj.info
                    moneda_codigo = info.get('currency', 'USD')
                    precio_actual = datos['Close'].iloc[-1]
                    
                    simbolos_moneda = {
                        "USD": "$", "EUR": "€", "GBP": "£", "GBp": "GBp",
                        "JPY": "¥", "HKD": "HK$", "MXN": "MX$", "SEK": "kr", "CHF": "CHF"
                    }
                    s_moneda = simbolos_moneda.get(moneda_codigo, moneda_codigo)
                    texto_mostrar = f"{precio_actual:.2f} {s_moneda}"
                    
                    if moneda_codigo != "USD":
                        try:
                            query_curr = "GBP" if moneda_codigo == "GBp" else moneda_codigo
                            factor = 0.01 if moneda_codigo == "GBp" else 1.0
                            tasa_fx = yf.Ticker(f"{query_curr}USD=X").history(period="1d")
                            if not tasa_fx.empty:
                                rate = tasa_fx['Close'].iloc[-1]
                                precio_usd = precio_actual * factor * rate
                                texto_mostrar = f"{precio_actual:.2f} {s_moneda} (${precio_usd:.2f})"
                        except Exception: pass
                    else:
                        texto_mostrar = f"{precio_actual:.2f} $"

                    st.metric(label=f"Valor Actual ({simbolo_real})", value=texto_mostrar)
                    
                    sector = info.get('sector', '')
                    industria = info.get('industry', '')
                    resumen_largo = info.get('longBusinessSummary', '')
                    
                    if sector and industria:
                        st.caption(f"🏢 **Sector:** {sector} | **Industria:** {industria}")
                    
                    if resumen_largo:
                        frases = resumen_largo.split('. ')
                        resumen_corto = '. '.join(frases[:2]) + '.' if len(frases) > 2 else resumen_largo
                        st.markdown(f"*{resumen_corto}*")
                    
                    st.write("")
                    
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=datos.index, y=datos['Close'], mode='lines', name='Precio', line=dict(color='#228B22', width=2)))
                    fig.update_layout(title=f"Cotización: {ticker_elegido}", template='plotly_dark', margin=dict(l=0, r=0, t=40, b=0), xaxis_title="", yaxis_title=f"Precio ({s_moneda})", hovermode="x unified")
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("⚠️ No hay datos históricos en Yahoo Finance para este activo en este periodo.")
            except Exception as e:
                st.error("⚠️ Ha ocurrido un error al cargar la gráfica y los datos corporativos.")

# ------------------------------------------
# PESTAÑA 2: EL RADAR DE CAZA 
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
        tickers_a_escanear = [t for t in tickers_nombres.keys() if mercado_objetivo == "Todos" or obtener_region(t) == mercado_objetivo]
        
        st.info(f"Iniciando Radar Cuantitativo V4 Adaptativo para: **{mercado_objetivo}** ({len(tickers_a_escanear)} activos)...")
        
        barra_progreso = st.progress(0, text="Iniciando conexión con Wall Street y calculando benchmark (SPY)...")
        resultados_radar = []
        
        alphaSPY = 0
        try:
            spy_hist = yf.Ticker("SPY").history(period="1mo")
            if len(spy_hist) >= 21:
                alphaSPY = ((spy_hist['Close'].iloc[-1] / spy_hist['Close'].iloc[-21]) - 1) * 100
        except Exception: pass

        for i, ticker in enumerate(tickers_a_escanear):
            porcentaje = int(((i + 1) / len(tickers_a_escanear)) * 100)
            nombre_empresa = tickers_nombres[ticker]
            region_activa = obtener_region(ticker)
            
            barra_progreso.progress(porcentaje, text=f"⏳ Evaluando: {ticker} ({nombre_empresa}) | {porcentaje}%")
            
            try:
                sym_yahoo = a_yahoo(ticker)
                stock = yf.Ticker(sym_yahoo)
                
                hist_full = stock.history(period="max")
                if hist_full.empty or len(hist_full) < 2: continue
                
                precio_actual = float(hist_full['Close'].iloc[-1])
                precio_ayer = float(hist_full['Close'].iloc[-2])
                pct_hoy = ((precio_actual / precio_ayer) - 1) * 100
                
                hist_1y = hist_full.iloc[-252:] if len(hist_full) >= 252 else hist_full
                max_52 = float(hist_1y['High'].max())
                min_52 = float(hist_1y['Low'].min())
                
                dist_suelo = ((precio_actual / min_52) - 1) * 100
                dist_max = ((precio_actual / max_52) - 1) * 100
                
                ret_1m = ((precio_actual / hist_full['Close'].iloc[-21]) - 1) * 100 if len(hist_full) >= 21 else 0
                ret_6m = ((precio_actual / hist_full['Close'].iloc[-126]) - 1) * 100 if len(hist_full) >= 126 else 0
                ret_1y = ((precio_actual / hist_full['Close'].iloc[-252]) - 1) * 100 if len(hist_full) >= 252 else 0
                ret_max = ((precio_actual / hist_full['Close'].iloc[0]) - 1) * 100
                
                info = stock.info
                sector = info.get('sector', 'N/A')
                per = info.get('trailingPE', 999)
                vol_hoy = float(hist_full['Volume'].iloc[-1])
                vol_medio = float(hist_full['Volume'].tail(20).mean())
                
                ptsBase = 50
                
                if ret_1m > 0 and ret_6m > 0: ptsBase += 15
                elif ret_1m > 0 and ret_6m < -15: ptsBase -= 20
                elif ret_6m <= 0: ptsBase -= 15
                else: ptsBase += 5
                
                myAlpha = ret_1m - (alphaSPY if region_activa == "EEUU" else 0)
                if myAlpha > 5: ptsBase += 10
                elif myAlpha < -5: ptsBase -= 10
                
                isHyperGrowth = (myAlpha > 10 and vol_medio > 1000000)
                
                if region_activa == "EEUU":
                    if 0 < per <= 45: ptsBase += 15
                    elif 45 < per <= 120 and isHyperGrowth: ptsBase += 15
                    elif per > 120 or per < 0: ptsBase -= 15
                elif region_activa == "Europa":
                    if 0 < per <= 15: ptsBase += 15
                    elif 15 < per <= 35 and isHyperGrowth: ptsBase += 15
                    elif per > 35 or per < 0: ptsBase -= 15
                elif region_activa == "Asia":
                    if 0 < per <= 30: ptsBase += 15
                    elif 30 < per <= 80 and isHyperGrowth: ptsBase += 15
                    elif per > 80 or per < 0: ptsBase -= 15
                
                if abs(pct_hoy) > 4 and vol_hoy < vol_medio: 
                    ptsBase -= 15 
                
                if region_activa == "EEUU" or region_activa == "Europa":
                    if abs(pct_hoy) <= 1.5 and vol_hoy >= (vol_medio * 1.5): ptsBase += 20
                    elif dist_max > -5 and vol_hoy >= (vol_medio * 2.0) and pct_hoy > 2: ptsBase += 25
                    elif dist_max > -2 and vol_hoy > (vol_medio * 1.5) and pct_hoy > 0: ptsBase += 15
                elif region_activa == "Asia":
                    if abs(pct_hoy) <= 2.0 and vol_hoy >= (vol_medio * 2.0): ptsBase += 20
                    elif dist_max > -10 and vol_hoy >= (vol_medio * 2.5) and pct_hoy > 3: ptsBase += 25
                    elif dist_max > -5 and vol_hoy > (vol_medio * 1.8) and pct_hoy > 0: ptsBase += 15
                
                isFenix = False
                if dist_max <= -20 and per > 0 and vol_medio > 400000 and ret_1m > 2 and pct_hoy > 1.5:
                    fuerzaGiro = (15 if ret_1m > 8 else 5) + (15 if vol_hoy > vol_medio * 1.2 else 0) + (10 if pct_hoy > 2 else 5)
                    scoreFenix = 65 + fuerzaGiro
                    if scoreFenix > ptsBase: 
                        ptsBase = scoreFenix
                        isFenix = True
                
                pts = max(0, min(100, int(ptsBase)))

                recomendacion = "❌ Esperar"
                if pts >= 65:
                    if pts >= 85: 
                        recomendacion = "🔥 COMPRA (FÉNIX ORO)" if isFenix else "🚀 COMPRA INSTITUCIONAL"
                    elif pts >= 70: 
                        recomendacion = "👀 VIGILAR (FÉNIX)" if isFenix else "💎 VIGILAR (BREAKOUT/WHALE)"
                
                moneda = info.get('currency', 'USD')
                simbolos_moneda = {"USD": "$", "EUR": "€", "GBP": "£", "GBp": "GBp", "JPY": "¥"}
                s_mon = simbolos_moneda.get(moneda, moneda)

                resultados_radar.append({
                    "Ticker": ticker,
                    "Nombre": nombre_empresa,
                    "PUNTOS": pts,
                    "RECOMENDACIÓN": recomendacion,
                    "PRECIO": f"{precio_actual:.2f} {s_mon}",
                    "% HOY": f"{pct_hoy:+.2f}%",
                    "% 1 mes": f"{ret_1m:+.2f}%",
                    "% 6 meses": f"{ret_6m:+.2f}%",
                    "% 1 año": f"{ret_1y:+.2f}%",
                    "% Máx": f"{ret_max:+.2f}%",
                    "PER": f"{per:.1f}" if per != 999 else "N/A",
                    "Sector": sector,
                    "Volumen": f"{vol_hoy:,.0f}",
                    "Vol. Medio": f"{vol_medio:,.0f}",
                    "Suelo (52s)": f"{dist_suelo:+.2f}%",
                    "Max (52s)": f"{dist_max:+.2f}%"
                })
                
            except Exception: continue
            
        barra_progreso.progress(100, text="✅ 100% Completado")
        
        if resultados_radar:
            df = pd.DataFrame(resultados_radar)
            df = df.sort_values(by="PUNTOS", ascending=False).reset_index(drop=True)
            
            # --- ESTILOS DE LA TABLA (AJUSTE VISUAL FINAL) ---
            def color_porcentajes(val):
                if isinstance(val, str) and '%' in val:
                    # Verde oscuro idéntico a la gráfica, SIN negrita
                    if val.startswith('+'): return 'color: #228B22;' 
                    # Rojo estándar, SIN negrita
                    elif val.startswith('-'): return 'color: #FF3333;' 
                return ''

            def negrita_ticker(val):
                # Solo negrita, sin forzar el color a blanco
                return 'font-weight: bold;' 

            columnas_pct = ["% HOY", "% 1 mes", "% 6 meses", "% 1 año", "% Máx", "Suelo (52s)", "Max (52s)"]
            
            try:
                styled_df = df.style.map(color_porcentajes, subset=columnas_pct)\
                                    .map(negrita_ticker, subset=['Ticker'])
            except AttributeError:
                styled_df = df.style.applymap(color_porcentajes, subset=columnas_pct)\
                                    .applymap(negrita_ticker, subset=['Ticker'])

            st.success("Caza terminada. Tu Matriz Cuantitativa V4 (Adaptativa) está lista:")
            
            st.dataframe(
                styled_df, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "PUNTOS": st.column_config.NumberColumn(help="Motor Quant V4 Adaptativo: Evalúa Alpha relativo, Indulto Tech y Rastros de Ballena según la región."),
                    "RECOMENDACIÓN": st.column_config.TextColumn(help="Etiquetas oficiales: Compra Institucional (>85) y Vigilar Breakout (>70)."),
                    "% HOY": st.column_config.TextColumn(help="Variación del precio en la sesión actual."),
                    "% 1 mes": st.column_config.TextColumn(help="Rendimiento en los últimos 21 días laborables."),
                    "% 6 meses": st.column_config.TextColumn(help="Rendimiento en los últimos 126 días laborables."),
                    "% 1 año": st.column_config.TextColumn(help="Rendimiento en el último año natural."),
                    "% Máx": st.column_config.TextColumn(help="Rendimiento histórico total."),
                    "PER": st.column_config.TextColumn(help="Price-to-Earnings. El Motor V4 indulta los PER altos si hay hipercrecimiento, ajustado por mercado."),
                    "Sector": st.column_config.TextColumn(help="Sector económico."),
                    "Volumen": st.column_config.TextColumn(help="Volumen última sesión."),
                    "Vol. Medio": st.column_config.TextColumn(help="Volumen medio 20 días."),
                    "Suelo (52s)": st.column_config.TextColumn(help="Distancia al precio MÍNIMO del último año."),
                    "Max (52s)": st.column_config.TextColumn(help="Distancia al precio MÁXIMO del último año.")
                }
            )
        else:
            st.error("No se han podido descargar datos en este momento.")
