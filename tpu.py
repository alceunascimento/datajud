"""
tpu.py — enriquecimento dos Parquets com dados da API TPU (SGT/CNJ).

Fluxo:
1. Localiza os Parquets mais recentes em data/parsed/
2. Baixa as tabelas completas de classes, assuntos e movimentos da API TPU
3. Usa DuckDB para fazer LEFT JOIN por código
4. Salva novos Parquets enriquecidos com sufixo _tpu

Colunas adicionadas (prefixo tpu_):
  tpu_nome, tpu_cod_item_pai, tpu_natureza, tpu_sigla,
  tpu_descricao, tpu_situacao, tpu_norma
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import duckdb
import requests

from config import DATA_DIR, PARSED_DIR, TPU_ENDPOINTS

log = logging.getLogger(__name__)

# colunas da resposta TPU que queremos preservar (comuns a classes/assuntos)
_TPU_COLS = [
    "cod_item",
    "cod_item_pai",
    "nome",
    "natureza",
    "sigla",
    "descricao_glossario",
    "norma",
    "situacao",
]

# movimentos usa "id" como PK em vez de "cod_item"
_TPU_COLS_MOV = ["id", "cod_item_pai", "nome", "descricao_glossario", "norma", "situacao"]

# renomeia para prefixo tpu_ (exceto a chave de join)
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

    # --- localiza parquets mais recentes ---
    parquets = _encontrar_parquets(parsed_dir)
    _log(f"[TPU] Parquets base: { {k: v.name for k,v in parquets.items()} }")

    # --- baixa tabelas TPU ---
    _log("[TPU] Baixando tabela de classes...")
    tpu_classes = _baixar_tpu("classes", _log)

    _log("[TPU] Baixando tabela de assuntos...")
    tpu_assuntos = _baixar_tpu("assuntos", _log)

    _log("[TPU] Baixando tabela de movimentos...")
    tpu_movimentos = _baixar_tpu("movimentos", _log)

    # --- enriquece via DuckDB ---
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit = '4GB'")

    _log("[TPU] Carregando tabelas TPU no DuckDB...")
    # classes/assuntos usam "cod_item"; movimentos usa "id"
    _registrar_tpu(con, "tpu_classes",    tpu_classes,    pk="cod_item", cols=_TPU_COLS,    rename=_TPU_RENAME)
    _registrar_tpu(con, "tpu_assuntos",   tpu_assuntos,   pk="cod_item", cols=_TPU_COLS,    rename=_TPU_RENAME)
    _registrar_tpu(con, "tpu_movimentos", tpu_movimentos, pk="id",       cols=_TPU_COLS_MOV, rename=_TPU_RENAME_MOV)

    resultados: dict[str, Path] = {}

    # enriquece processos (join por classe_codigo → tpu_classes.cod_item)
    out = parsed_dir / f"processos_{ts}_tpu.parquet"
    _enriquecer_parquet(
        con=con, src=parquets["processos"],
        tpu_table="tpu_classes", join_col="classe_codigo",
        tpu_rename=_TPU_RENAME, out=out, _log=_log, label="processos",
    )
    resultados["processos_tpu"] = out

    # enriquece assuntos (join por assunto_codigo → tpu_assuntos.cod_item)
    out = parsed_dir / f"assuntos_{ts}_tpu.parquet"
    _enriquecer_parquet(
        con=con, src=parquets["assuntos"],
        tpu_table="tpu_assuntos", join_col="assunto_codigo",
        tpu_rename=_TPU_RENAME, out=out, _log=_log, label="assuntos",
    )
    resultados["assuntos_tpu"] = out

    # enriquece movimentos (join por movimento_codigo → tpu_movimentos.id)
    out = parsed_dir / f"movimentos_{ts}_tpu.parquet"
    _enriquecer_parquet(
        con=con, src=parquets["movimentos"],
        tpu_table="tpu_movimentos", join_col="movimento_codigo",
        tpu_rename=_TPU_RENAME_MOV, out=out, _log=_log, label="movimentos",
    )
    resultados["movimentos_tpu"] = out

    con.close()
    _log("[TPU] Enriquecimento concluído.")
    return resultados


# ── helpers internos ──────────────────────────────────────────────────────────

def _encontrar_parquets(parsed_dir: Path) -> dict[str, Path]:
    """Retorna o Parquet mais recente de cada tipo (processos/assuntos/movimentos)."""
    resultado: dict[str, Path] = {}
    for tipo in ("processos", "assuntos", "movimentos"):
        # exclui arquivos já enriquecidos (_tpu)
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


def _baixar_tpu(tipo: str, _log: Callable) -> list[dict]:
    """GET na API TPU e retorna lista de dicts."""
    url = TPU_ENDPOINTS[tipo]
    try:
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(
                f"API TPU retornou HTTP {r.status_code} para {tipo}. "
                f"Verifique TPU_BASE_URL em config.py. Body: {r.text[:300]}"
            )
        data = r.json()
        _log(f"[TPU] {tipo}: {len(data)} itens recebidos.")
        return data
    except requests.RequestException as exc:
        raise RuntimeError(f"Falha ao acessar API TPU ({tipo}): {exc}") from exc


def _registrar_tpu(
    con: duckdb.DuckDBPyConnection,
    table: str,
    data: list[dict],
    pk: str,
    cols: list[str],
    rename: dict[str, str],
) -> None:
    """Cria tabela DuckDB a partir da lista TPU.

    pk    — nome da chave primária no JSON ('cod_item' ou 'id')
    cols  — colunas a preservar
    rename — mapeamento col → tpu_col (exceto pk)
    """
    rows = []
    for item in data:
        row = {}
        for col in cols:
            val = item.get(col)
            if col == "situacao" and isinstance(val, dict):
                val = val.get("descricao") or val.get("nome") or json.dumps(val, ensure_ascii=False)
            # normaliza pk: sempre exposto como "cod_item" para o JOIN ser uniforme
            row["cod_item" if col == pk else col] = val
        rows.append(row)

    import pandas as pd
    df = pd.DataFrame(rows)
    df = df.rename(columns=rename)
    con.register(table, df)


def _enriquecer_parquet(
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
        data = _baixar_tpu(tipo, _log)

        # converte para DataFrame mantendo TODAS as colunas (sem filtro)
        import pandas as pd
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
        n = len(df)
        _log(f"[TPU] {tipo}: {n:,} itens → {out.name}")
        resultados[tipo] = out

    _log("[TPU] Download completo finalizado.")
    return resultados


# ── Classificação de movimentos ───────────────────────────────────────────────

# Códigos-âncora na árvore TPU de movimentos.
# Um movimento recebe True numa categoria se ele mesmo OU qualquer
# ancestral seu tiver o código correspondente.
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
    Lê o movimentos parquet mais recente, classifica cada linha
    com booleanos baseados na árvore TPU e salva
    movimentos_{ts}_class.parquet.

    Retorna o path do arquivo gerado.
    """
    import pandas as pd

    def _log(msg: str) -> None:
        if progress_cb:
            progress_cb(msg)
        else:
            log.info(msg)

    # 1. parquet de movimentos base
    mov_src = _encontrar_parquets(parsed_dir)["movimentos"]
    _log(f"[CLASS] Fonte: {mov_src.name}")

    # 2. árvore TPU de movimentos
    #    usa tpu_movimentos_*.parquet se já baixado, senão baixa agora
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
        raw = _baixar_tpu("movimentos", _log)
        tpu_df = pd.DataFrame(raw)[["id", "cod_item_pai"]]

    # 3. monta mapa {codigo → codigo_pai}
    parent: dict[int, int | None] = {}
    for _, row in tpu_df.iterrows():
        pk  = row.get("id")
        pai = row.get("cod_item_pai")
        if pk is not None and not pd.isna(pk):
            parent[int(pk)] = int(pai) if (pai is not None and not pd.isna(pai)) else None

    _log(f"[CLASS] Árvore TPU carregada: {len(parent)} nós")

    # 4. cache de ancestrais (inclui o próprio nó)
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

    # 5. carrega movimentos e classifica
    _log("[CLASS] Classificando movimentos...")
    df = pd.read_parquet(mov_src)

    for col, anchor in _ANCHOR_CODES.items():
        df[col] = df["movimento_codigo"].map(
            lambda c, a=anchor: (a in ancestors(int(c))) if pd.notna(c) else False
        )

    # 6. salva
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    out = parsed_dir / f"movimentos_{ts}_class.parquet"
    df.to_parquet(out, index=False)

    n_total = len(df)
    counts = {col: int(df[col].sum()) for col in _ANCHOR_CODES}
    _log(f"[CLASS] {n_total:,} movimentos classificados → {out.name}")
    for col, n in counts.items():
        _log(f"[CLASS]   {col}: {n:,}")

    return out
