"""
api.py — cliente HTTP para a API Pública DataJud (CNJ).

Responsabilidades:
- POST com backoff exponencial em 429/503
- Paginação via search_after (nunca from+size)
- Contagem prévia sem consumir resultados
"""
import logging
import time
from typing import Generator

import requests

from config import BASE_URL, HEADERS, MAX_RETRIES, PAGE_SIZE, REQUEST_TIMEOUT

_RETRY_STATUS = {429, 500, 502, 503, 504}

log = logging.getLogger(__name__)


def _endpoint(tribunal_alias: str) -> str:
    return BASE_URL.format(tribunal=tribunal_alias.lower())


def count(tribunal_alias: str, query_body: dict) -> int:
    """Retorna total de hits sem baixar dados."""
    body = {
        "size": 0,
        "query": query_body.get("query", {"match_all": {}}),
        "track_total_hits": True,
    }
    resp = _post(tribunal_alias, body)
    total = resp["hits"]["total"]
    return total["value"] if isinstance(total, dict) else int(total)


def search(
    tribunal_alias: str,
    query_body: dict,
    page_size: int = PAGE_SIZE,
) -> Generator[dict, None, None]:
    """
    Itera por todos os hits usando search_after.
    Yields cada hit (_source já normalizado de datas).
    """
    body = {
        **query_body,
        "size": page_size,
        # _id não tem doc_values no índice DataJud — sort só por @timestamp
        "sort": [{"@timestamp": {"order": "asc"}}],
    }
    after = None
    page = 0

    while True:
        if after is not None:
            body["search_after"] = after

        page += 1
        log.debug("[%s] página %d (after=%s)", tribunal_alias, page, after)

        t0 = time.monotonic()
        resp = _post(tribunal_alias, body)
        elapsed = time.monotonic() - t0
        hits = resp["hits"]["hits"]

        if not hits:
            log.info("[%s] sem mais resultados (páginas: %d)", tribunal_alias, page)
            return

        for hit in hits:
            yield hit

        after = hits[-1]["sort"]
        log.info(
            "[%s] pág %d → %d hits (size=%d, %.1fs)",
            tribunal_alias, page, len(hits), page_size, elapsed,
        )
        time.sleep(0.5)  # throttling defensivo


def _post(tribunal_alias: str, body: dict, max_retries: int = MAX_RETRIES) -> dict:
    """POST com backoff exponencial em 429/5xx/timeout. Erros 4xx (≠429) propagam."""
    url = _endpoint(tribunal_alias)
    size = body.get("size", "?")
    for attempt in range(max_retries + 1):
        try:
            t0 = time.monotonic()
            r = requests.post(url, headers=HEADERS, json=body, timeout=REQUEST_TIMEOUT)
            elapsed = time.monotonic() - t0

            # 4xx ≠ 429 = erro do cliente, não adianta retry
            if 400 <= r.status_code < 500 and r.status_code != 429:
                log.error(
                    "HTTP %d em %s (size=%s, %.1fs) — erro do cliente. Body: %s",
                    r.status_code, tribunal_alias, size, elapsed, r.text[:500],
                )
                r.raise_for_status()

            if r.status_code in _RETRY_STATUS:
                wait = min(2 ** attempt, 60)
                log.warning(
                    "HTTP %d em %s (size=%s, %.1fs) — aguardando %ds (tentativa %d/%d)",
                    r.status_code, tribunal_alias, size, elapsed, wait,
                    attempt + 1, max_retries,
                )
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except requests.HTTPError:
            raise
        except requests.RequestException as exc:
            if attempt >= max_retries:
                log.error("Falha após %d tentativas em %s: %s", max_retries, tribunal_alias, exc)
                raise
            wait = min(2 ** attempt, 60)
            log.warning(
                "Erro de rede em %s (size=%s): %s — aguardando %ds (tentativa %d/%d)",
                tribunal_alias, size, exc, wait, attempt + 1, max_retries,
            )
            time.sleep(wait)

    raise RuntimeError(f"_post falhou após {max_retries} retentativas ({tribunal_alias})")
