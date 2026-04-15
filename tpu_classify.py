"""
tpu_classify.py — classifica movimentos pela árvore hierárquica TPU.

Para cada movimento, marca 9 booleanos (magistrado, serventuário, decisão,
despacho, julgamento com/sem mérito, etc.) com base nos códigos-âncora da
árvore TPU: um movimento recebe True se ele próprio OU qualquer ancestral
seu na árvore tiver o código-âncora da categoria.

Gera movimentos_{ts}_class.parquet a partir do movimentos_{ts}.parquet base.
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from config import DATA_DIR, PARSED_DIR
from tpu_client import baixar as tpu_client_baixar
from tpu_enrich import encontrar_parquets_base

log = logging.getLogger(__name__)

# Códigos-âncora na árvore TPU de movimentos.
_ANCHOR_CODES: dict[str, int] = {
    "magistrado":                            1,
    "serventuario":                         14,
    "escrivao":                             48,
    "decisao":                               3,
    "despacho":                          11009,
    "julgamento_com_resolucao_do_merito":  385,
    "julgamento_sem_resolucao_do_merito":  218,
    "oficial_justica_devolucao_mandado":   106,
    "oficial_justica_recebimento_mandado": 985,
}


def classificar_movimentos(
    parsed_dir: Path = PARSED_DIR,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> Path:
    """
    Lê o movimentos parquet mais recente, classifica cada linha com booleanos
    baseados na árvore TPU e salva movimentos_{ts}_class.parquet.
    """
    import pandas as pd

    def _log(msg: str) -> None:
        if progress_cb:
            progress_cb(msg)
        else:
            log.info(msg)

    # 1. parquet de movimentos base
    mov_src = encontrar_parquets_base(parsed_dir)["movimentos"]
    _log(f"[CLASS] Fonte: {mov_src.name}")

    # 2. árvore TPU de movimentos (reusa parquet local se existir)
    parent = _carregar_arvore(pd, _log)

    # 3. classifica
    _log("[CLASS] Classificando movimentos...")
    df = pd.read_parquet(mov_src)
    _cache: dict[int, frozenset[int]] = {}

    def ancestors(code: int) -> frozenset[int]:
        if code in _cache:
            return _cache[code]
        chain: set[int] = set()
        cur: int | None = code
        seen: set[int] = set()
        while cur is not None and cur not in seen:
            chain.add(cur)
            seen.add(cur)
            cur = parent.get(cur)
        result = frozenset(chain)
        _cache[code] = result
        return result

    for col, anchor in _ANCHOR_CODES.items():
        df[col] = df["movimento_codigo"].map(
            lambda c, a=anchor: (a in ancestors(int(c))) if pd.notna(c) else False
        )

    # 4. salva
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    out = parsed_dir / f"movimentos_{ts}_class.parquet"
    df.to_parquet(out, index=False)

    n_total = len(df)
    counts = {col: int(df[col].sum()) for col in _ANCHOR_CODES}
    _log(f"[CLASS] {n_total:,} movimentos classificados → {out.name}")
    for col, n in counts.items():
        _log(f"[CLASS]   {col}: {n:,}")

    return out


def _carregar_arvore(pd, _log: Callable) -> dict[int, int | None]:
    """Carrega o mapa {codigo → codigo_pai} da árvore TPU de movimentos.

    Reusa tpu_movimentos_*.parquet em DATA_DIR se já baixado; caso contrário,
    baixa sob demanda.
    """
    tpu_files = sorted(
        DATA_DIR.glob("tpu_movimentos_*.parquet"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if tpu_files:
        _log(f"[CLASS] Usando árvore TPU: {tpu_files[0].name}")
        tpu_df = pd.read_parquet(tpu_files[0], columns=["id", "cod_item_pai"])
    else:
        _log("[CLASS] tpu_movimentos não encontrado, baixando...")
        raw = tpu_client_baixar("movimentos", _log)
        tpu_df = pd.DataFrame(raw)[["id", "cod_item_pai"]]

    parent: dict[int, int | None] = {}
    for _, row in tpu_df.iterrows():
        pk  = row.get("id")
        pai = row.get("cod_item_pai")
        if pk is not None and not pd.isna(pk):
            parent[int(pk)] = int(pai) if (pai is not None and not pd.isna(pai)) else None

    _log(f"[CLASS] Árvore TPU carregada: {len(parent)} nós")
    return parent
