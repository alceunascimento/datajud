"""
tpu_client.py — cliente HTTP para a API TPU do CNJ.

Responsabilidade única: fazer GET nas 3 tabelas (classes, assuntos, movimentos)
e devolver lista de dicts. Sem parsing, sem persistência, sem regra de negócio.
"""
import logging
from typing import Callable

import requests

from config import TPU_ENDPOINTS

log = logging.getLogger(__name__)


def baixar(tipo: str, progress: Callable[[str], None] = log.info) -> list[dict]:
    """GET na API TPU para o tipo ('classes' | 'assuntos' | 'movimentos').

    Retorna lista de dicts. Lança RuntimeError em erro HTTP ou de rede.
    """
    url = TPU_ENDPOINTS[tipo]
    try:
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(
                f"API TPU retornou HTTP {r.status_code} para {tipo}. "
                f"Verifique TPU_BASE_URL em config.py. Body: {r.text[:300]}"
            )
        data = r.json()
        progress(f"[TPU] {tipo}: {len(data)} itens recebidos.")
        return data
    except requests.RequestException as exc:
        raise RuntimeError(f"Falha ao acessar API TPU ({tipo}): {exc}") from exc
