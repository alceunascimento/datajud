"""
ingestor.py — orquestra a coleta de dados da API e persiste em NDJSON.

Estratégia de memória:
- Cada página (search_after) é imediatamente descarregada em disco como NDJSON.
- A normalização de datas ocorre aqui, antes de salvar — o parser lê dados limpos.
- Nunca acumula todos os resultados em RAM.
"""
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import api
from config import RAW_DIR

log = logging.getLogger(__name__)

# Pares (regex, strptime_format) — mais específico primeiro.
# Regex é necessário: strptime aceita 1 dígito em %M/%S, causando parse errado.
_DATE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$"),   "%Y-%m-%dT%H:%M:%S.%fZ"),
    (re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"),         "%Y-%m-%dT%H:%M:%SZ"),
    (re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+$"),     "%Y-%m-%dT%H:%M:%S.%f"),
    (re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"),          "%Y-%m-%dT%H:%M:%S"),
    (re.compile(r"^\d{14}$"),                                          "%Y%m%d%H%M%S"),
    (re.compile(r"^\d{12}$"),                                          "%Y%m%d%H%M"),
    (re.compile(r"^\d{8}$"),                                           "%Y%m%d"),
]


def _normalize_date(value) -> Optional[str]:
    """Converte qualquer formato de data DataJud para ISO8601 sem fuso."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    for pattern, fmt in _DATE_PATTERNS:
        if pattern.match(s):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue
    log.debug("Data não reconhecida, mantendo raw: %s", s)
    return s


def _normalize_source(src: dict) -> dict:
    """Normaliza campos de data dentro de _source antes de salvar."""
    # data de ajuizamento
    if "dataAjuizamento" in src:
        src["dataAjuizamento"] = _normalize_date(src["dataAjuizamento"])

    # datas em movimentos
    for mov in src.get("movimentos", []):
        if "dataHora" in mov:
            mov["dataHora"] = _normalize_date(mov["dataHora"])

    return src


def coletar(
    tribunal_alias: str,
    query_body: dict,
    out_dir: Path = RAW_DIR,
    page_size: int = 1000,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> list[Path]:
    """
    Coleta todos os resultados de um tribunal para uma query.
    Salva cada página como um arquivo NDJSON separado.
    Retorna lista de arquivos criados.
    """
    def _log(msg: str) -> None:
        if progress_cb:
            progress_cb(msg)
        else:
            log.info(msg)

    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    arquivos: list[Path] = []
    page = 0
    buf: list[str] = []

    _log(f"[{tribunal_alias.upper()}] Iniciando coleta...")

    total = api.count(tribunal_alias, query_body)
    _log(f"[{tribunal_alias.upper()}] Total estimado: {total} processos")

    for hit in api.search(tribunal_alias, query_body, page_size=page_size):
        src = _normalize_source(hit["_source"])
        buf.append(json.dumps(src, ensure_ascii=False))

        if len(buf) >= page_size:
            page += 1
            path = _flush(buf, out_dir, tribunal_alias, page, ts)
            arquivos.append(path)
            _log(f"[{tribunal_alias.upper()}] pág {page} → {len(buf)} registros → {path.name}")
            buf.clear()

    # flush do restante
    if buf:
        page += 1
        path = _flush(buf, out_dir, tribunal_alias, page, ts)
        arquivos.append(path)
        _log(f"[{tribunal_alias.upper()}] pág {page} (final) → {len(buf)} registros → {path.name}")

    _log(f"[{tribunal_alias.upper()}] Coleta concluída: {page} página(s), {len(arquivos)} arquivo(s).")
    return arquivos


def coletar_multiplos(
    tribunal_aliases: list[str],
    query_body: dict,
    out_dir: Path = RAW_DIR,
    page_size: int = 1000,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> list[Path]:
    """Itera por múltiplos tribunais e coleta todos."""
    todos: list[Path] = []
    for alias in tribunal_aliases:
        try:
            arquivos = coletar(alias, query_body, out_dir, page_size, progress_cb)
            todos.extend(arquivos)
        except Exception as exc:
            msg = f"[{alias.upper()}] ERRO na coleta: {exc}"
            log.error(msg)
            if progress_cb:
                progress_cb(msg)
    return todos


def _flush(buf: list[str], out_dir: Path, alias: str, page: int, ts: str) -> Path:
    path = out_dir / f"datajud_{alias}_{ts}_p{page:04d}.ndjson"
    path.write_text("\n".join(buf), encoding="utf-8")
    return path
