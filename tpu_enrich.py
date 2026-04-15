"""
tpu_enrich.py — LEFT JOIN dos Parquets parseados com as tabelas TPU.

Para cada um dos 3 parquets (processos, assuntos, movimentos), faz o JOIN
pelo código correspondente contra a tabela TPU e salva um novo parquet
com sufixo _tpu. Colunas originais preservadas via SELECT p.*.
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import duckdb

from config import PARSED_DIR
from tpu_client import baixar as tpu_client_baixar

log = logging.getLogger(__name__)

# colunas da resposta TPU que queremos preservar (comuns a classes/assuntos)
_TPU_COLS = [
    "cod_item", "cod_item_pai", "nome", "natureza", "sigla",
    "descricao_glossario", "norma", "situacao",
]

# movimentos usa "id" como PK em vez de "cod_item"
_TPU_COLS_MOV = ["id", "cod_item_pai", "nome", "descricao_glossario", "norma", "situacao"]

_TPU_RENAME     = {c: f"tpu_{c}" for c in _TPU_COLS    if c != "cod_item"}
_TPU_RENAME_MOV = {c: f"tpu_{c}" for c in _TPU_COLS_MOV if c != "id"}


def enriquecer(
    parsed_dir: Path = PARSED_DIR,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict[str, Path]:
    """
    Localiza os Parquets mais recentes, enriquece com TPU e salva novos arquivos.
    Retorna dict com caminhos dos Parquets enriquecidos.
    """
    def _log(msg: str) -> None:
        if progress_cb:
            progress_cb(msg)
        else:
            log.info(msg)

    parquets = encontrar_parquets_base(parsed_dir)
    _log(f"[TPU] Parquets base: { {k: v.name for k, v in parquets.items()} }")

    _log("[TPU] Baixando tabela de classes...")
    tpu_classes = tpu_client_baixar("classes", _log)

    _log("[TPU] Baixando tabela de assuntos...")
    tpu_assuntos = tpu_client_baixar("assuntos", _log)

    _log("[TPU] Baixando tabela de movimentos...")
    tpu_movimentos = tpu_client_baixar("movimentos", _log)

    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit = '4GB'")

    _log("[TPU] Carregando tabelas TPU no DuckDB...")
    _registrar_tpu(con, "tpu_classes",    tpu_classes,    pk="cod_item", cols=_TPU_COLS,    rename=_TPU_RENAME)
    _registrar_tpu(con, "tpu_assuntos",   tpu_assuntos,   pk="cod_item", cols=_TPU_COLS,    rename=_TPU_RENAME)
    _registrar_tpu(con, "tpu_movimentos", tpu_movimentos, pk="id",       cols=_TPU_COLS_MOV, rename=_TPU_RENAME_MOV)

    resultados: dict[str, Path] = {}

    out = parsed_dir / f"processos_{ts}_tpu.parquet"
    _join(con, parquets["processos"], "tpu_classes", "classe_codigo",
          _TPU_RENAME, out, _log, "processos")
    resultados["processos_tpu"] = out

    out = parsed_dir / f"assuntos_{ts}_tpu.parquet"
    _join(con, parquets["assuntos"], "tpu_assuntos", "assunto_codigo",
          _TPU_RENAME, out, _log, "assuntos")
    resultados["assuntos_tpu"] = out

    out = parsed_dir / f"movimentos_{ts}_tpu.parquet"
    _join(con, parquets["movimentos"], "tpu_movimentos", "movimento_codigo",
          _TPU_RENAME_MOV, out, _log, "movimentos")
    resultados["movimentos_tpu"] = out

    con.close()
    _log("[TPU] Enriquecimento concluído.")
    return resultados


def encontrar_parquets_base(parsed_dir: Path) -> dict[str, Path]:
    """Retorna o Parquet base (não _tpu, não _class) mais recente de cada tipo."""
    resultado: dict[str, Path] = {}
    for tipo in ("processos", "assuntos", "movimentos"):
        candidatos = sorted(
            [
                p for p in parsed_dir.glob(f"{tipo}_*.parquet")
                if not any(s in p.name for s in ("_tpu", "_class"))
            ],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not candidatos:
            raise FileNotFoundError(
                f"Nenhum Parquet de '{tipo}' encontrado em {parsed_dir}. "
                "Execute o parse antes."
            )
        resultado[tipo] = candidatos[0]
    return resultado


# ── helpers internos ──────────────────────────────────────────────────────────

def _registrar_tpu(
    con: duckdb.DuckDBPyConnection,
    table: str,
    data: list[dict],
    pk: str,
    cols: list[str],
    rename: dict[str, str],
) -> None:
    """Cria tabela DuckDB a partir da lista TPU.

    pk     — nome da chave primária no JSON ('cod_item' ou 'id')
    cols   — colunas a preservar
    rename — mapeamento col → tpu_col (exceto pk)
    """
    rows = []
    for item in data:
        row = {}
        for col in cols:
            val = item.get(col)
            if col == "situacao" and isinstance(val, dict):
                val = val.get("descricao") or val.get("nome") or json.dumps(val, ensure_ascii=False)
            # PK sempre exposta como "cod_item" para o JOIN ser uniforme
            row["cod_item" if col == pk else col] = val
        rows.append(row)

    import pandas as pd
    df = pd.DataFrame(rows).rename(columns=rename)
    con.register(table, df)


def _join(
    con: duckdb.DuckDBPyConnection,
    src: Path,
    tpu_table: str,
    join_col: str,
    tpu_rename: dict[str, str],
    out: Path,
    _log: Callable,
    label: str,
) -> None:
    """LEFT JOIN do Parquet original com a tabela TPU e exporta."""
    _log(f"[TPU] Enriquecendo {label} (join {join_col} → {tpu_table}.cod_item)...")
    try:
        tpu_cols_sel = ", ".join(f"t.{v}" for v in tpu_rename.values())
        con.execute(f"""
            COPY (
                SELECT
                    p.*,
                    {tpu_cols_sel}
                FROM read_parquet('{src}') p
                LEFT JOIN {tpu_table} t
                    ON p.{join_col} = t.cod_item
            ) TO '{out}' (FORMAT PARQUET)
        """)
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]
        _log(f"[TPU] {label}: {n:,} registros enriquecidos → {out.name}")
    except Exception as exc:
        log.error("[TPU] Erro ao enriquecer %s: %s", label, exc)
        raise
