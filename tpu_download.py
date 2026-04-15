"""
tpu_download.py — baixa as 3 tabelas TPU completas e salva como Parquet cru.

Sem join, sem enriquecimento. Apenas dump das tabelas TPU para inspeção e
reuso (p.ex. a árvore de movimentos é consumida pelo classificador).
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from config import DATA_DIR
from tpu_client import baixar as tpu_client_baixar

log = logging.getLogger(__name__)


def baixar_completa(
    out_dir: Path = DATA_DIR,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict[str, Path]:
    """
    Baixa as tabelas TPU completas (classes, assuntos, movimentos) e salva
    como Parquets em out_dir sem nenhum join.

    Arquivos gerados:
      tpu_classes_{ts}.parquet
      tpu_assuntos_{ts}.parquet
      tpu_movimentos_{ts}.parquet
    """
    import pandas as pd

    def _log(msg: str) -> None:
        if progress_cb:
            progress_cb(msg)
        else:
            log.info(msg)

    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    resultados: dict[str, Path] = {}

    for tipo in ("classes", "assuntos", "movimentos"):
        _log(f"[TPU] Baixando {tipo}...")
        data = tpu_client_baixar(tipo, _log)
        df = pd.DataFrame(data)

        # situacao pode ser objeto — serializa para string
        if "situacao" in df.columns:
            df["situacao"] = df["situacao"].apply(
                lambda v: (
                    v.get("descricao") or v.get("nome") or json.dumps(v, ensure_ascii=False)
                    if isinstance(v, dict) else v
                )
            )

        out = out_dir / f"tpu_{tipo}_{ts}.parquet"
        df.to_parquet(out, index=False)
        _log(f"[TPU] {tipo}: {len(df):,} itens → {out.name}")
        resultados[tipo] = out

    _log("[TPU] Download completo finalizado.")
    return resultados
