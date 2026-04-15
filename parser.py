"""
parser.py — converte NDJSON brutos (data/raw/) em 3 Parquets (data/parsed/).

Usa DuckDB para:
- ler todos os NDJSON de uma vez (in-memory, com limite configurável)
- UNNEST de assuntos e movimentos (incluindo complementosTabelados)
- exportar diretamente para Parquet (sem passar por pandas)

As datas já chegam normalizadas pelo ingestor (ISO8601), então não há
problema de inferência de tipo no DuckDB.
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import duckdb

from config import RAW_DIR, PARSED_DIR

log = logging.getLogger(__name__)

MAX_JSON_BYTES = 100_000_000  # 100 MB por objeto JSON


def parsear(
    raw_dir: Path = RAW_DIR,
    out_dir: Path = PARSED_DIR,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict[str, Path]:
    """
    Lê todos os NDJSON em raw_dir e exporta 3 Parquets em out_dir.
    Retorna dict com chaves 'processos', 'assuntos', 'movimentos'.
    """

    def _log(msg: str) -> None:
        if progress_cb:
            progress_cb(msg)
        else:
            log.info(msg)

    out_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(raw_dir.glob("*.ndjson"))

    if not files:
        raise FileNotFoundError(f"Nenhum NDJSON encontrado em {raw_dir}")

    _log(f"[PARSER] {len(files)} arquivo(s) NDJSON encontrado(s).")

    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    out_processos  = out_dir / f"processos_{ts}.parquet"
    out_assuntos   = out_dir / f"assuntos_{ts}.parquet"
    out_movimentos = out_dir / f"movimentos_{ts}.parquet"

    files_expr = ", ".join(f"'{f}'" for f in files)

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit = '4GB'")
    con.execute("SET threads = 4")
    # macro para lidar com todos os formatos de data do DataJud
    con.execute("""
        CREATE MACRO parse_dt(s) AS COALESCE(
            TRY_CAST(s AS TIMESTAMP),
            TRY_STRPTIME(CAST(s AS VARCHAR), '%Y%m%d%H%M%S'),
            TRY_STRPTIME(CAST(s AS VARCHAR), '%Y%m%d%H%M'),
            TRY_STRPTIME(CAST(s AS VARCHAR), '%Y%m%d')
        )
    """)
    _log("[PARSER] DuckDB conectado.")

    _exportar_processos(con, files_expr, out_processos, _log)
    _exportar_assuntos(con, files_expr, out_assuntos, _log)
    _exportar_movimentos(con, files_expr, out_movimentos, _log)

    con.close()
    _log("[PARSER] Concluído.")

    return {
        "processos":  out_processos,
        "assuntos":   out_assuntos,
        "movimentos": out_movimentos,
    }


# ── tabelas individuais ───────────────────────────────────────────────────────

def _exportar_processos(
    con: duckdb.DuckDBPyConnection,
    files_expr: str,
    out: Path,
    _log: Callable,
) -> None:
    _log("[PARSER] Parseando processos...")
    try:
        con.execute(f"""
            COPY (
                SELECT
                    id                                      AS id,
                    id_local                                AS id_local,
                    numeroProcesso                          AS numero_processo,
                    classe.codigo                           AS classe_codigo,
                    classe.nome                             AS classe_nome,
                    sistema.codigo                          AS sistema_codigo,
                    sistema.nome                            AS sistema_nome,
                    formato.codigo                          AS formato_codigo,
                    formato.nome                            AS formato_nome,
                    tribunal,
                    grau,
                    TRY_CAST(nivelSigilo AS INTEGER)        AS nivel_sigilo,
                    parse_dt(dataAjuizamento)               AS data_ajuizamento,
                    orgaoJulgador.codigo                    AS orgao_julgador_codigo,
                    orgaoJulgador.nome                      AS orgao_julgador_nome,
                    TRY_CAST(orgaoJulgador.codigoMunicipioIBGE AS VARCHAR)
                                                            AS orgao_municipio_ibge,
                    TRY_CAST("@timestamp" AS TIMESTAMP)     AS ts_index
                FROM read_json(
                    [{files_expr}],
                    format = 'newline_delimited',
                    maximum_object_size = {MAX_JSON_BYTES}
                )
            ) TO '{out}' (FORMAT PARQUET)
        """)
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]
        _log(f"[PARSER] processos: {n:,} registros → {out.name}")
    except Exception as exc:
        log.error("[PARSER] Erro em processos: %s", exc)
        raise


def _exportar_assuntos(
    con: duckdb.DuckDBPyConnection,
    files_expr: str,
    out: Path,
    _log: Callable,
) -> None:
    _log("[PARSER] Parseando assuntos...")
    try:
        con.execute(f"""
            COPY (
                SELECT
                    id                       AS id,
                    id_local                 AS id_local,
                    numeroProcesso           AS numero_processo,
                    a.codigo                 AS assunto_codigo,
                    REPLACE(a.nome, '"', '') AS assunto_nome
                FROM read_json(
                    [{files_expr}],
                    format = 'newline_delimited',
                    maximum_object_size = {MAX_JSON_BYTES}
                ),
                UNNEST(assuntos) AS t(a)
            ) TO '{out}' (FORMAT PARQUET)
        """)
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]
        _log(f"[PARSER] assuntos: {n:,} registros → {out.name}")
    except Exception as exc:
        log.error("[PARSER] Erro em assuntos: %s", exc)
        raise


def _exportar_movimentos(
    con: duckdb.DuckDBPyConnection,
    files_expr: str,
    out: Path,
    _log: Callable,
) -> None:
    _log("[PARSER] Parseando movimentos...")
    try:
        con.execute(f"""
            COPY (
                SELECT
                    id                                          AS id,
                    id_local                                    AS id_local,
                    numeroProcesso                              AS numero_processo,
                    m.codigo                                    AS movimento_codigo,
                    REPLACE(m.nome, '"', '')                    AS movimento_nome,
                    parse_dt(m.dataHora)                        AS movimento_data_hora,
                    c.codigo                                    AS complemento_codigo,
                    REPLACE(COALESCE(c.nome, ''), '"', '')      AS complemento_nome,
                    REPLACE(CAST(TRY_CAST(c.valor AS VARCHAR) AS VARCHAR), '"', '')
                                                                AS complemento_valor,
                    REPLACE(COALESCE(c.descricao, ''), '"', '') AS complemento_descricao
                FROM read_json(
                    [{files_expr}],
                    format = 'newline_delimited',
                    maximum_object_size = {MAX_JSON_BYTES}
                ),
                UNNEST(movimentos) AS t(m)
                LEFT JOIN UNNEST(m.complementosTabelados) AS u(c) ON true
            ) TO '{out}' (FORMAT PARQUET)
        """)
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]
        _log(f"[PARSER] movimentos: {n:,} registros → {out.name}")
    except Exception as exc:
        log.error("[PARSER] Erro em movimentos: %s", exc)
        raise
