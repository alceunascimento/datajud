"""
config.py — constantes globais do projeto DataJud.
"""
from pathlib import Path

BASE_DIR   = Path(__file__).parent
DATA_DIR   = BASE_DIR / "data"
RAW_DIR    = DATA_DIR / "raw"
PARSED_DIR = DATA_DIR / "parsed"
LOGS_DIR   = BASE_DIR / "logs"

# Garante que os diretórios existem no import
for _d in (RAW_DIR, PARSED_DIR, LOGS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# API pública CNJ — chave deliberadamente pública (Resolução CNJ 331/2020)
API_KEY  = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="
BASE_URL = "https://api-publica.datajud.cnj.jus.br/api_publica_{tribunal}/_search"

HEADERS = {
    "Authorization": f"APIKey {API_KEY}",
    "Content-Type": "application/json",
}

PAGE_SIZE = 1000          # hits por página (search_after)
REQUEST_TIMEOUT = 300     # segundos; páginas grandes (10k) podem demorar > 60s
MAX_RETRIES = 6           # tentativas em 429/502/503/504/erros de rede

# API TPU — PJe Cloud Gateway (https://gateway.cloud.pje.jus.br/tpu)
# Sem parâmetros: retorna tabela completa
# Com ?codigo=N: retorna o item e seus descendentes na hierarquia
TPU_BASE_URL = "https://gateway.cloud.pje.jus.br/tpu"
TPU_ENDPOINTS = {
    "classes":    f"{TPU_BASE_URL}/api/v1/publico/download/classes",
    "assuntos":   f"{TPU_BASE_URL}/api/v1/publico/download/assuntos",
    "movimentos": f"{TPU_BASE_URL}/api/v1/publico/download/movimentos",
}

# Mapa completo tribunal → alias (fonte: datajud-wiki.cnj.jus.br/api-publica/endpoints)
TRIBUNAIS: dict[str, str] = {
    # Superiores
    "TST":   "tst",
    "TSE":   "tse",
    "STJ":   "stj",
    "STM":   "stm",
    # Justiça Federal
    "TRF1":  "trf1",
    "TRF2":  "trf2",
    "TRF3":  "trf3",
    "TRF4":  "trf4",
    "TRF5":  "trf5",
    "TRF6":  "trf6",
    # Justiça do Trabalho
    **{f"TRT{i}": f"trt{i}" for i in range(1, 25)},
    # Justiça Estadual
    "TJAC":  "tjac",
    "TJAL":  "tjal",
    "TJAM":  "tjam",
    "TJAP":  "tjap",
    "TJBA":  "tjba",
    "TJCE":  "tjce",
    "TJDFT": "tjdft",
    "TJES":  "tjes",
    "TJGO":  "tjgo",
    "TJMA":  "tjma",
    "TJMG":  "tjmg",
    "TJMS":  "tjms",
    "TJMT":  "tjmt",
    "TJPA":  "tjpa",
    "TJPB":  "tjpb",
    "TJPE":  "tjpe",
    "TJPI":  "tjpi",
    "TJPR":  "tjpr",
    "TJRJ":  "tjrj",
    "TJRN":  "tjrn",
    "TJRO":  "tjro",
    "TJRR":  "tjrr",
    "TJRS":  "tjrs",
    "TJSC":  "tjsc",
    "TJSE":  "tjse",
    "TJSP":  "tjsp",
    "TJTO":  "tjto",
    # Justiça Eleitoral
    **{f"TRE-{uf}": f"tre-{uf.lower()}" for uf in [
        "AC","AL","AM","AP","BA","CE","DF","ES","GO",
        "MA","MG","MS","MT","PA","PB","PE","PI","PR",
        "RJ","RN","RO","RR","RS","SC","SE","SP","TO",
    ]},
    # Justiça Militar Estadual
    "TJMMG": "tjmmg",
    "TJMRS": "tjmrs",
    "TJMSP": "tjmsp",
}
