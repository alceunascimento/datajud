"""
cli.py — interface de linha de comando para o DataJud Query Tool.

Despacho feito por main.py quando sys.argv tem argumentos.
Não importa Tkinter — seguro para rodar via SSH sem display.

Uso geral:
    python main.py <comando> [opções]
    python main.py --help
    python main.py coletar --help
"""
import argparse
import csv
import logging
import sys
from pathlib import Path

import query as Q
import ingestor
import magistrados as datajud_magistrados
import parser as datajud_parser
import tpu as datajud_tpu
from config import LOGS_DIR, PARSED_DIR, RAW_DIR, TRIBUNAIS


def run_cli() -> None:
    """Ponto de entrada CLI — parseia args e despacha."""
    parser = _build_parser()
    args = parser.parse_args()

    if args.comando is None:
        parser.print_help()
        sys.exit(0)

    _setup_logging()

    try:
        _dispatch(args)
    except KeyboardInterrupt:
        logging.info("Interrompido pelo usuário.")
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as exc:
        logging.error("Erro fatal: %s", exc)
        sys.exit(1)


# ── logging ───────────────────────────────────────────────────────────────────

def _setup_logging() -> None:
    """Logging para stdout + arquivo em modo CLI."""
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fmt_console = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", "%H:%M:%S")
    fmt_file    = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s")

    h_console = logging.StreamHandler(sys.stdout)
    h_console.setFormatter(fmt_console)
    root.addHandler(h_console)

    h_file = logging.FileHandler(LOGS_DIR / "datajud.log", encoding="utf-8")
    h_file.setFormatter(fmt_file)
    root.addHandler(h_file)


# ── parser de argumentos ──────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    main_parser = argparse.ArgumentParser(
        prog="datajud",
        description="DataJud Query Tool — extração e análise de dados processuais CNJ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
exemplos:
  # processo único
  python main.py coletar --tipo processo --numeros 00008323520184013202 --tribunais TRF1

  # classe com período, múltiplos tribunais
  python main.py coletar --tipo classe --codigos 436 --tribunais TJPR TJSP --de 2024-01-01 --ate 2024-12-31

  # múltiplas classes via CSV
  python main.py coletar --tipo classe --csv classes.csv --tribunais TJPR

  # query combinada
  python main.py coletar --tipo combinada --classes 436 --assuntos 6177 --tribunais TJPR

  # pipeline completo
  python main.py parsear
  python main.py enriquecer
  python main.py classificar

  # em background com nohup
  nohup python main.py coletar --tipo classe --codigos 436 --tribunais TJPR > logs/run.log 2>&1 &
        """,
    )

    sub = main_parser.add_subparsers(dest="comando", metavar="comando")

    # ── coletar ──────────────────────────────────────────────────────────────
    p_col = sub.add_parser(
        "coletar",
        help="Coleta dados da API DataJud e salva NDJSON em data/raw/",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
tipos de query:
  processo   --numeros N1 N2 ...   (ou --csv)
  classe     --codigos C1 C2 ...   (ou --csv)
  assunto    --codigos C1 C2 ...   (ou --csv)
  orgao      --codigos C1 C2 ...   (ou --csv)
  combinada  --classes C1 --assuntos A1 --orgaos O1 --numeros N1 ...
        """,
    )
    p_col.add_argument(
        "--tipo",
        choices=["processo", "classe", "assunto", "orgao", "municipio", "combinada"],
        required=True,
        metavar="TIPO",
        help="processo | classe | assunto | orgao | municipio | combinada",
    )
    p_col.add_argument("--numeros",  nargs="+", metavar="NUM",
                       help="Número(s) CNJ de processo (20 dígitos)")
    p_col.add_argument("--codigos",  nargs="+", type=int, metavar="COD",
                       help="Código(s) TPU — para tipo processo/classe/assunto/orgao")
    p_col.add_argument("--classes",  nargs="+", type=int, metavar="COD",
                       help="[combinada] Código(s) de classe")
    p_col.add_argument("--assuntos", nargs="+", type=int, metavar="COD",
                       help="[combinada] Código(s) de assunto")
    p_col.add_argument("--orgaos",   nargs="+", type=int, metavar="COD",
                       help="[combinada] Código(s) de órgão julgador")
    p_col.add_argument("--municipios", nargs="+", type=int, metavar="COD",
                       help="[combinada] Código(s) IBGE de município")
    p_col.add_argument("--csv", type=Path, metavar="ARQUIVO",
                       help="CSV com códigos/números na 1ª coluna (substitui --codigos/--numeros)")
    p_col.add_argument("--tribunais", nargs="+", required=True, metavar="TRB",
                       help="Siglas dos tribunais, ex: TJPR TJSP TRF1")
    p_col.add_argument("--de",  dest="date_gte", metavar="YYYY-MM-DD",
                       help="Data de ajuizamento inicial (inclusive)")
    p_col.add_argument("--ate", dest="date_lt",  metavar="YYYY-MM-DD",
                       help="Data de ajuizamento final (exclusive)")

    # ── parsear ───────────────────────────────────────────────────────────────
    sub.add_parser(
        "parsear",
        help="Converte NDJSON de data/raw/ em Parquets em data/parsed/",
    )

    # ── enriquecer ────────────────────────────────────────────────────────────
    sub.add_parser(
        "enriquecer",
        help="Enriquece os Parquets mais recentes com colunas TPU (LEFT JOIN)",
    )

    # ── classificar ───────────────────────────────────────────────────────────
    sub.add_parser(
        "classificar",
        help="Classifica movimentos com 9 booleanos pela árvore hierárquica TPU",
    )

    # ── baixar-tpu ────────────────────────────────────────────────────────────
    sub.add_parser(
        "baixar-tpu",
        help="Baixa tabelas TPU completas (classes/assuntos/movimentos) para data/",
    )

    # ── magistrados-tjpr ──────────────────────────────────────────────────────
    sub.add_parser(
        "magistrados-tjpr",
        help="Baixa base de magistrados + unidades do TJPR em Parquet",
    )

    return main_parser


# ── despacho ──────────────────────────────────────────────────────────────────

def _dispatch(args: argparse.Namespace) -> None:
    cmd = args.comando

    if cmd == "coletar":
        _cmd_coletar(args)

    elif cmd == "parsear":
        result = datajud_parser.parsear(raw_dir=RAW_DIR, out_dir=PARSED_DIR)
        for k, p in result.items():
            logging.info("Parquet gerado: %s → %s", k, p)

    elif cmd == "enriquecer":
        result = datajud_tpu.enriquecer(parsed_dir=PARSED_DIR)
        for k, p in result.items():
            logging.info("Parquet TPU: %s → %s", k, p)

    elif cmd == "classificar":
        out = datajud_tpu.classificar_movimentos(parsed_dir=PARSED_DIR)
        logging.info("Classificação salva em: %s", out)

    elif cmd == "baixar-tpu":
        result = datajud_tpu.baixar_completa()
        for k, p in result.items():
            logging.info("TPU completa: %s → %s", k, p)

    elif cmd == "magistrados-tjpr":
        result = datajud_magistrados.baixar(out_dir=PARSED_DIR)
        for k, p in result.items():
            logging.info("Magistrados TJPR: %s → %s", k, p)


# ── comando coletar ───────────────────────────────────────────────────────────

def _cmd_coletar(args: argparse.Namespace) -> None:
    aliases  = _resolve_tribunais(args.tribunais)
    date_gte = (args.date_gte + "T00:00:00.000Z") if args.date_gte else None
    date_lt  = (args.date_lt  + "T00:00:00.000Z") if args.date_lt  else None

    # CSV sobrescreve --codigos / --numeros
    csv_values: list[str] = _read_csv(args.csv) if args.csv else []
    if csv_values:
        logging.info("CSV carregado: %d valores de %s", len(csv_values), args.csv)

    tipo = args.tipo
    query_body: dict

    if tipo == "processo":
        nums = csv_values or (args.numeros or [])
        if not nums:
            _die("--tipo processo requer --numeros ou --csv")
        query_body = (Q.por_numero_processo(nums[0]) if len(nums) == 1
                      else Q.por_numeros_processo(nums))

    elif tipo == "classe":
        cods = [int(v) for v in csv_values] if csv_values else (args.codigos or [])
        if not cods:
            _die("--tipo classe requer --codigos ou --csv")
        query_body = (Q.por_classe(cods[0], date_gte=date_gte, date_lt=date_lt) if len(cods) == 1
                      else Q.por_classes(cods, date_gte=date_gte, date_lt=date_lt))

    elif tipo == "assunto":
        cods = [int(v) for v in csv_values] if csv_values else (args.codigos or [])
        if not cods:
            _die("--tipo assunto requer --codigos ou --csv")
        query_body = (Q.por_assunto(cods[0], date_gte=date_gte, date_lt=date_lt) if len(cods) == 1
                      else Q.por_assuntos(cods, date_gte=date_gte, date_lt=date_lt))

    elif tipo == "orgao":
        cods = [int(v) for v in csv_values] if csv_values else (args.codigos or [])
        if not cods:
            _die("--tipo orgao requer --codigos ou --csv")
        query_body = (Q.por_orgao(cods[0], date_gte=date_gte, date_lt=date_lt) if len(cods) == 1
                      else Q.por_orgaos(cods, date_gte=date_gte, date_lt=date_lt))

    elif tipo == "municipio":
        cods = [int(v) for v in csv_values] if csv_values else (args.codigos or [])
        if not cods:
            _die("--tipo municipio requer --codigos ou --csv (código IBGE)")
        query_body = (Q.por_municipio(cods[0], date_gte=date_gte, date_lt=date_lt) if len(cods) == 1
                      else Q.por_municipios(cods, date_gte=date_gte, date_lt=date_lt))

    elif tipo == "combinada":
        nums       = csv_values if csv_values else (args.numeros or [])
        classes    = args.classes    or []
        assuntos   = args.assuntos   or []
        orgaos     = args.orgaos     or []
        municipios = args.municipios or []
        if not any([nums, classes, assuntos, orgaos, municipios]):
            _die("--tipo combinada requer pelo menos um filtro")
        query_body = Q.combinada(
            numeros=nums       or None,
            classes=classes    or None,
            assuntos=assuntos  or None,
            orgaos=orgaos      or None,
            municipios=municipios or None,
            date_gte=date_gte,
            date_lt=date_lt,
        )

    else:
        _die(f"Tipo desconhecido: {tipo}")

    ingestor.coletar_multiplos(tribunal_aliases=aliases, query_body=query_body)


# ── helpers ───────────────────────────────────────────────────────────────────

def _resolve_tribunais(nomes: list[str]) -> list[str]:
    """Aceita siglas (TJPR, tjpr) ou aliases diretos (tjpr). Retorna aliases."""
    aliases = []
    for nome in nomes:
        upper = nome.upper()
        if upper in TRIBUNAIS:
            aliases.append(TRIBUNAIS[upper])
        else:
            # assume alias direto (ex: trf1 passado em lowercase)
            aliases.append(nome.lower())
    return aliases


def _read_csv(path: Path) -> list[str]:
    """Lê 1ª coluna de CSV, descarta cabeçalho não numérico."""
    values: list[str] = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        for i, row in enumerate(csv.reader(f)):
            if not row:
                continue
            val = row[0].strip()
            if i == 0 and not val.replace("-", "").isdigit():
                continue  # cabeçalho
            if val:
                values.append(val)
    return values


def _die(msg: str) -> None:
    logging.error(msg)
    sys.exit(1)
