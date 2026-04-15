"""
magistrados.py — coleta lista completa de magistrados do TJPR.

Fonte: https://portal.tjpr.jus.br/magistratura/api/listaCompleta
Saída: data/parsed/magistrados_tjpr_YYYYMMDDHHMMSS.parquet
       data/parsed/unidades_tjpr_YYYYMMDDHHMMSS.parquet

Todas as linhas carregam `dt_referencia` (data da coleta) para permitir
acompanhamento intertemporal — magistrados mudam de comarca/entrância.
"""
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
import requests

from config import PARSED_DIR

log = logging.getLogger(__name__)

API_URL    = "https://portal.tjpr.jus.br/magistratura/api/listaCompleta"
TIMEOUT_S  = 120
MAX_RETRY  = 3
RETRY_WAIT = 5

_HEADERS = {
    "Accept": "application/json",
    "Accept-Encoding": "identity",
    "User-Agent": "datajud-query-tool/python",
}

_RE_SEDE = re.compile(r"Comarca da Regi.o Metropolitana de (.+)")
_RE_NOME = re.compile(r"Comarca da Regi.o Metropolitana de .+ - Foro (Central|Regional) de (.+)")


def _parse_sede(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Extrai nome da RM quando sede segue padrão 'Comarca da Região Metropolitana de X'."""
    if not raw:
        return None, raw
    m = _RE_SEDE.match(raw)
    if m:
        return raw, m.group(1).upper()
    return None, raw


def _parse_nome(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Extrai cidade do foro quando nome segue padrão '... - Foro Central/Regional de X'."""
    if not raw:
        return None, raw
    m = _RE_NOME.match(raw)
    if m:
        return raw, m.group(2).upper()
    return None, raw


def _fetch(url: str = API_URL,
           progress_cb: Optional[Callable[[str], None]] = None) -> list:
    """GET com retry. Retorna JSON decodificado (lista de comarcas-sede)."""
    def _log(msg: str) -> None:
        log.info(msg)
        if progress_cb:
            progress_cb(msg)

    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRY + 1):
        _log(f"[TJPR-MAG] Tentativa {attempt}/{MAX_RETRY} — GET {url}")
        try:
            r = requests.get(url, headers=_HEADERS, timeout=TIMEOUT_S)
            if r.status_code == 200:
                _log(f"[TJPR-MAG] HTTP 200 — {len(r.content):,} bytes")
                return r.json()
            _log(f"[TJPR-MAG] HTTP {r.status_code} — retry em {RETRY_WAIT}s")
        except requests.RequestException as exc:
            last_exc = exc
            _log(f"[TJPR-MAG] Erro: {exc} — retry em {RETRY_WAIT}s")
        time.sleep(RETRY_WAIT)

    raise RuntimeError(f"Falha após {MAX_RETRY} tentativas em {url}: {last_exc}")


def _flatten_unidades(data: list, dt_ref: str) -> pd.DataFrame:
    """Desaninha estrutura em linhas de unidades judiciárias."""
    rows = []
    for sede in data:
        sede_raw, sede_parsed = _parse_sede(sede.get("comarcaSede"))
        for comarca in (sede.get("comarcas") or []):
            nome_raw, nome_parsed = _parse_nome(comarca.get("nome"))
            cid = comarca.get("id")
            cent = comarca.get("entrancia")
            for uj in (comarca.get("unidadesJudiciais") or []):
                rows.append({
                    "dt_referencia":     dt_ref,
                    "comarca_sede_raw":  sede_raw,
                    "comarca_sede":      sede_parsed,
                    "comarca_nome_raw":  nome_raw,
                    "comarca_nome":      nome_parsed,
                    "comarca_id":        cid,
                    "comarca_entrancia": cent,
                    "unidade_id":        uj.get("id"),
                    "unidade_nome":      uj.get("nome"),
                    "unidade_email":     uj.get("email"),
                    "unidade_id_domus":  uj.get("idDomus"),
                    "secao_judiciaria":  uj.get("secaoJudiciaria"),
                })
    return pd.DataFrame(rows)


def _flatten_magistrados(data: list, dt_ref: str) -> pd.DataFrame:
    """Desaninha em linhas de magistrados (titulares + substitutos)."""
    rows = []
    for sede in data:
        sede_raw, sede_parsed = _parse_sede(sede.get("comarcaSede"))

        for comarca in (sede.get("comarcas") or []):
            nome_raw, nome_parsed = _parse_nome(comarca.get("nome"))
            cid = comarca.get("id")
            for uj in (comarca.get("unidadesJudiciais") or []):
                juiz = uj.get("juizTitular")
                if juiz and str(juiz).strip():
                    rows.append({
                        "dt_referencia":    dt_ref,
                        "comarca_sede_raw": sede_raw,
                        "comarca_sede":     sede_parsed,
                        "comarca_nome_raw": nome_raw,
                        "comarca_nome":     nome_parsed,
                        "comarca_id":       cid,
                        "unidade_nome":     uj.get("nome"),
                        "magistrado_id":    None,
                        "magistrado_nome":  juiz,
                        "perfil":           "titular",
                    })

        for j in (sede.get("juizesSubstitutos") or []):
            rows.append({
                "dt_referencia":    dt_ref,
                "comarca_sede_raw": sede_raw,
                "comarca_sede":     sede_parsed,
                "comarca_nome_raw": None,
                "comarca_nome":     None,
                "comarca_id":       None,
                "unidade_nome":     "sem informacao de vinculo",
                "magistrado_id":    j.get("id"),
                "magistrado_nome":  j.get("nomeCompleto") or j.get("nome"),
                "perfil":           "substituto",
            })

    return pd.DataFrame(rows)


def baixar(out_dir: Path = PARSED_DIR,
           progress_cb: Optional[Callable[[str], None]] = None) -> dict[str, Path]:
    """
    Baixa e persiste magistrados + unidades do TJPR em Parquet.
    Retorna dict com chaves 'magistrados' e 'unidades'.
    """
    def _log(msg: str) -> None:
        log.info(msg)
        if progress_cb:
            progress_cb(msg)

    out_dir.mkdir(parents=True, exist_ok=True)
    now     = datetime.now()
    ts      = now.strftime("%Y%m%d%H%M%S")
    dt_ref  = now.strftime("%Y-%m-%d")

    data = _fetch(progress_cb=progress_cb)

    n_sede = len(data)
    n_com  = sum(len(s.get("comarcas") or []) for s in data)
    n_uj   = sum(
        len(c.get("unidadesJudiciais") or [])
        for s in data for c in (s.get("comarcas") or [])
    )
    _log(f"[TJPR-MAG] Comarcas-sede: {n_sede} | Comarcas: {n_com} | Unidades: {n_uj}")

    df_u = _flatten_unidades(data, dt_ref)
    df_m = _flatten_magistrados(data, dt_ref)

    path_m = out_dir / f"magistrados_tjpr_{ts}.parquet"
    path_u = out_dir / f"unidades_tjpr_{ts}.parquet"
    df_m.to_parquet(path_m, index=False, compression="zstd")
    df_u.to_parquet(path_u, index=False, compression="zstd")

    n_tit = int((df_m["perfil"] == "titular").sum())    if not df_m.empty else 0
    n_sub = int((df_m["perfil"] == "substituto").sum()) if not df_m.empty else 0
    _log(f"[TJPR-MAG] magistrados: {len(df_m)} linhas (titulares={n_tit}, substitutos={n_sub}) → {path_m.name}")
    _log(f"[TJPR-MAG] unidades:    {len(df_u)} linhas → {path_u.name}")

    return {"magistrados": path_m, "unidades": path_u}
