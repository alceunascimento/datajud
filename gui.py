"""
gui.py — interface gráfica Tkinter para o DataJud Query Tool.

Layout:
  ┌────────────────────────────────────────────────────────┐
  │  [Tabs: Processo | Classe | Assunto | Órgão | Combinada]
  ├────────────────────────────┬───────────────────────────┤
  │  Parâmetros da query       │  Tribunais + Datas        │
  ├────────────────────────────┴───────────────────────────┤
  │  [EXECUTAR]  [PARSEAR]  [LIMPAR LOG]                   │
  ├────────────────────────────────────────────────────────┤
  │  Log (ScrolledText)                                    │
  └────────────────────────────────────────────────────────┘
"""
import csv
import logging
import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Callable

import ingestor
import magistrados as datajud_magistrados
import parser as datajud_parser
import query as Q
import tpu as datajud_tpu
from config import PARSED_DIR, RAW_DIR, TRIBUNAIS

# ── logging → queue → GUI ────────────────────────────────────────────────────

class _QueueHandler(logging.Handler):
    def __init__(self, q: queue.Queue) -> None:
        super().__init__()
        self._q = q

    def emit(self, record: logging.LogRecord) -> None:
        self._q.put(self.format(record))


def _setup_logging(q: queue.Queue) -> None:
    root_log = logging.getLogger()
    root_log.setLevel(logging.DEBUG)
    handler = _QueueHandler(q)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
    # evita handlers duplicados se a GUI for reiniciada
    root_log.handlers = [h for h in root_log.handlers if not isinstance(h, _QueueHandler)]
    root_log.addHandler(handler)


# ── Janela principal ──────────────────────────────────────────────────────────

class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("DataJud Query Tool")
        self.geometry("1100x780")
        self.resizable(True, True)

        self._log_queue: queue.Queue = queue.Queue()
        self._enable_after_parse: bool = False  # flag: habilita botões pós-parse
        _setup_logging(self._log_queue)

        self._build_ui()
        self._poll_log()

    # ── construção do layout ─────────────────────────────────────────────────

    def _build_ui(self) -> None:
        # frame principal
        main = ttk.Frame(self, padding=8)
        main.pack(fill=tk.BOTH, expand=True)

        # topo: abas + tribunais lado a lado
        top = ttk.Frame(main)
        top.pack(fill=tk.BOTH, expand=False)

        # coluna esquerda: abas de query
        left = ttk.LabelFrame(top, text="Query", padding=6)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 4))

        self._notebook = ttk.Notebook(left)
        self._notebook.pack(fill=tk.BOTH, expand=True)

        self._tab_processo  = self._build_tab_processo()
        self._tab_classe    = self._build_tab_classe()
        self._tab_assunto   = self._build_tab_assunto()
        self._tab_orgao     = self._build_tab_orgao()
        self._tab_municipio = self._build_tab_municipio()
        self._tab_combinada = self._build_tab_combinada()

        # coluna direita: tribunais + datas
        right = ttk.LabelFrame(top, text="Tribunais & Datas", padding=6)
        right.pack(side=tk.RIGHT, fill=tk.BOTH, expand=False, ipadx=4)
        self._build_tribunais(right)

        # botões de ação — linha 1
        btn_frame1 = ttk.Frame(main, padding=(0, 3))
        btn_frame1.pack(fill=tk.X)
        ttk.Button(btn_frame1, text="▶ EXECUTAR QUERY", command=self._executar, width=22).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_frame1, text="⚙ PARSEAR NDJSON", command=self._parsear,  width=22).pack(side=tk.LEFT, padx=4)

        # botões habilitados após parse — guardados para enable em lote
        self._btn_tpu = ttk.Button(btn_frame1, text="★ ENRIQUECER TPU",
                                   command=self._enriquecer_tpu, width=20)
        self._btn_tpu.pack(side=tk.LEFT, padx=4)
        self._btn_tpu.state(["disabled"])

        self._btn_class = ttk.Button(btn_frame1, text="⚡ CLASSIFICAR MOV.",
                                     command=self._classificar_movimentos, width=20)
        self._btn_class.pack(side=tk.LEFT, padx=4)
        self._btn_class.state(["disabled"])

        # lista de botões a habilitar após parse
        self._btns_post_parse = [self._btn_tpu, self._btn_class]

        # botões de ação — linha 2
        btn_frame2 = ttk.Frame(main, padding=(0, 3))
        btn_frame2.pack(fill=tk.X)
        ttk.Button(btn_frame2, text="⬇ BAIXAR TPU COMPLETA", command=self._baixar_tpu_completa, width=24).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_frame2, text="👨‍⚖ MAGISTRADOS TJPR",  command=self._baixar_magistrados_tjpr, width=22).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_frame2, text="🗑 LIMPAR LOG",          command=self._limpar_log,          width=16).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_frame2, text="📂 Abrir data/",         command=self._abrir_data,          width=16).pack(side=tk.LEFT, padx=4)

        # log
        log_frame = ttk.LabelFrame(main, text="Log", padding=4)
        log_frame.pack(fill=tk.BOTH, expand=True)
        self._log_text = scrolledtext.ScrolledText(
            log_frame, state=tk.DISABLED, height=18,
            font=("Monospace", 9), bg="#1e1e1e", fg="#d4d4d4",
            insertbackground="white",
        )
        self._log_text.pack(fill=tk.BOTH, expand=True)

    # ── abas ─────────────────────────────────────────────────────────────────

    def _build_tab_processo(self) -> ttk.Frame:
        tab = ttk.Frame(self._notebook, padding=8)
        self._notebook.add(tab, text="  Processo  ")

        self._proc_mode = tk.StringVar(value="unico")
        ttk.Radiobutton(tab, text="Único", variable=self._proc_mode, value="unico",
                        command=self._update_proc_mode).grid(row=0, column=0, sticky=tk.W)
        ttk.Radiobutton(tab, text="Múltiplos (CSV)", variable=self._proc_mode, value="multiplo",
                        command=self._update_proc_mode).grid(row=0, column=1, sticky=tk.W)

        ttk.Label(tab, text="Número(s) CNJ:").grid(row=1, column=0, sticky=tk.W, pady=(6, 0))
        self._proc_entry = ttk.Entry(tab, width=50)
        self._proc_entry.grid(row=2, column=0, columnspan=2, sticky=tk.EW, pady=2)
        ttk.Label(tab, text="(20 dígitos sem máscara; múltiplos separados por vírgula)",
                  foreground="gray").grid(row=3, column=0, columnspan=2, sticky=tk.W)

        self._proc_csv_btn = ttk.Button(tab, text="Selecionar CSV...", command=self._load_proc_csv)
        self._proc_csv_btn.grid(row=4, column=0, sticky=tk.W, pady=4)
        self._proc_csv_btn.state(["disabled"])

        tab.columnconfigure(0, weight=1)
        return tab

    def _build_tab_classe(self) -> ttk.Frame:
        tab = ttk.Frame(self._notebook, padding=8)
        self._notebook.add(tab, text="  Classe  ")

        self._classe_mode = tk.StringVar(value="unico")
        ttk.Radiobutton(tab, text="Único", variable=self._classe_mode, value="unico").grid(row=0, column=0, sticky=tk.W)
        ttk.Radiobutton(tab, text="Múltiplos", variable=self._classe_mode, value="multiplo").grid(row=0, column=1, sticky=tk.W)

        ttk.Label(tab, text="Código(s) TPU de Classe:").grid(row=1, column=0, sticky=tk.W, pady=(6,0))
        self._classe_entry = ttk.Entry(tab, width=50)
        self._classe_entry.grid(row=2, column=0, columnspan=2, sticky=tk.EW, pady=2)
        ttk.Label(tab, text="Separados por vírgula. Ex: 436, 159", foreground="gray").grid(
            row=3, column=0, columnspan=2, sticky=tk.W)

        ttk.Button(tab, text="Carregar CSV...", command=lambda: self._load_csv(self._classe_entry)).grid(
            row=4, column=0, sticky=tk.W, pady=4)
        tab.columnconfigure(0, weight=1)
        return tab

    def _build_tab_assunto(self) -> ttk.Frame:
        tab = ttk.Frame(self._notebook, padding=8)
        self._notebook.add(tab, text="  Assunto  ")

        ttk.Label(tab, text="Código(s) TPU de Assunto:").grid(row=0, column=0, sticky=tk.W)
        self._assunto_entry = ttk.Entry(tab, width=50)
        self._assunto_entry.grid(row=1, column=0, columnspan=2, sticky=tk.EW, pady=2)
        ttk.Label(tab, text="Separados por vírgula. Ex: 10431, 6177", foreground="gray").grid(
            row=2, column=0, columnspan=2, sticky=tk.W)

        ttk.Button(tab, text="Carregar CSV...", command=lambda: self._load_csv(self._assunto_entry)).grid(
            row=3, column=0, sticky=tk.W, pady=4)
        tab.columnconfigure(0, weight=1)
        return tab

    def _build_tab_orgao(self) -> ttk.Frame:
        tab = ttk.Frame(self._notebook, padding=8)
        self._notebook.add(tab, text="  Órgão Julgador  ")

        ttk.Label(tab, text="Código(s) de Órgão Julgador:").grid(row=0, column=0, sticky=tk.W)
        self._orgao_entry = ttk.Entry(tab, width=50)
        self._orgao_entry.grid(row=1, column=0, columnspan=2, sticky=tk.EW, pady=2)
        ttk.Label(tab, text="Separados por vírgula. Ex: 12345, 16403", foreground="gray").grid(
            row=2, column=0, columnspan=2, sticky=tk.W)

        ttk.Button(tab, text="Carregar CSV...", command=lambda: self._load_csv(self._orgao_entry)).grid(
            row=3, column=0, sticky=tk.W, pady=4)
        tab.columnconfigure(0, weight=1)
        return tab

    def _build_tab_municipio(self) -> ttk.Frame:
        tab = ttk.Frame(self._notebook, padding=8)
        self._notebook.add(tab, text="  Município  ")

        ttk.Label(tab, text="Código(s) IBGE do Município:").grid(row=0, column=0, sticky=tk.W)
        self._municipio_entry = ttk.Entry(tab, width=50)
        self._municipio_entry.grid(row=1, column=0, columnspan=2, sticky=tk.EW, pady=2)
        ttk.Label(tab, text="Separados por vírgula. Ex: 3550308, 3304557", foreground="gray").grid(
            row=2, column=0, columnspan=2, sticky=tk.W)

        ttk.Button(tab, text="Carregar CSV...", command=lambda: self._load_csv(self._municipio_entry)).grid(
            row=3, column=0, sticky=tk.W, pady=4)
        ttk.Button(tab, text="🔎 Encontrar código do município",
                   command=self._abrir_ibge_municipios).grid(row=3, column=1, sticky=tk.W, pady=4)
        tab.columnconfigure(0, weight=1)
        return tab

    def _build_tab_combinada(self) -> ttk.Frame:
        tab = ttk.Frame(self._notebook, padding=8)
        self._notebook.add(tab, text="  Combinada  ")

        def _row(label: str, row: int) -> ttk.Entry:
            ttk.Label(tab, text=label).grid(row=row, column=0, sticky=tk.W, pady=(4,0))
            entry = ttk.Entry(tab, width=48)
            entry.grid(row=row + 1, column=0, columnspan=2, sticky=tk.EW, pady=2)
            return entry

        self._comb_proc    = _row("Números de processo (vírgula):", 0)
        self._comb_classes = _row("Códigos de classe (vírgula):", 2)
        self._comb_assuntos = _row("Códigos de assunto (vírgula):", 4)
        self._comb_orgaos  = _row("Códigos de órgão julgador (vírgula):", 6)
        self._comb_municipios = _row("Códigos IBGE de município (vírgula):", 8)

        ttk.Button(tab, text="🔎 Encontrar código do município",
                   command=self._abrir_ibge_municipios).grid(row=10, column=0, sticky=tk.W, pady=4)

        ttk.Label(tab, text="Preencha apenas os campos desejados.", foreground="gray").grid(
            row=11, column=0, columnspan=2, sticky=tk.W, pady=4)
        tab.columnconfigure(0, weight=1)
        return tab

    def _build_tribunais(self, parent: ttk.LabelFrame) -> None:
        # Filtro rápido por sigla — acima da lista
        ttk.Label(parent, text="Filtrar:").pack(anchor=tk.W)
        self._trib_filter = ttk.Entry(parent, width=18)
        self._trib_filter.pack(fill=tk.X, pady=(0, 4))
        self._trib_filter.bind("<KeyRelease>", self._filter_trib)

        # Seleção de tribunais
        select_frame = ttk.Frame(parent)
        select_frame.pack(fill=tk.BOTH, expand=True)

        scroll = ttk.Scrollbar(select_frame, orient=tk.VERTICAL)
        self._trib_list = tk.Listbox(
            select_frame, selectmode=tk.MULTIPLE,
            yscrollcommand=scroll.set,
            height=14, width=18,
            exportselection=False,
        )
        scroll.config(command=self._trib_list.yview)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self._trib_list.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self._tribunal_keys = sorted(TRIBUNAIS.keys())
        for t in self._tribunal_keys:
            self._trib_list.insert(tk.END, t)

        btn_trib = ttk.Frame(parent)
        btn_trib.pack(fill=tk.X, pady=(2, 6))
        ttk.Button(btn_trib, text="Todos", command=self._select_all_trib, width=8).pack(side=tk.LEFT)
        ttk.Button(btn_trib, text="Nenhum", command=self._clear_trib, width=8).pack(side=tk.LEFT, padx=2)

        # Datas
        date_frame = ttk.LabelFrame(parent, text="Período de ajuizamento", padding=4)
        date_frame.pack(fill=tk.X, pady=(8, 0))

        ttk.Label(date_frame, text="De (YYYY-MM-DD):").grid(row=0, column=0, sticky=tk.W)
        self._date_gte = ttk.Entry(date_frame, width=14)
        self._date_gte.grid(row=0, column=1, sticky=tk.W, padx=4)

        ttk.Label(date_frame, text="Até (YYYY-MM-DD):").grid(row=1, column=0, sticky=tk.W, pady=2)
        self._date_lt = ttk.Entry(date_frame, width=14)
        self._date_lt.grid(row=1, column=1, sticky=tk.W, padx=4)

        ttk.Label(date_frame, text="(vazio = sem filtro de data)", foreground="gray").grid(
            row=2, column=0, columnspan=2, sticky=tk.W)

        # Page size
        page_frame = ttk.LabelFrame(parent, text="Processos por página", padding=4)
        page_frame.pack(fill=tk.X, pady=(8, 0))
        self._page_size = tk.StringVar(value="1000")
        ttk.Combobox(page_frame, textvariable=self._page_size,
                     values=["1000", "5000", "10000"], width=10,
                     state="readonly").pack(anchor=tk.W)
        ttk.Label(page_frame, text="(máx. API = 10000)", foreground="gray").pack(anchor=tk.W)

    # ── helpers de tribunal ──────────────────────────────────────────────────

    def _select_all_trib(self) -> None:
        self._trib_list.select_set(0, tk.END)

    def _clear_trib(self) -> None:
        self._trib_list.selection_clear(0, tk.END)

    def _filter_trib(self, _event=None) -> None:
        term = self._trib_filter.get().upper()
        self._trib_list.delete(0, tk.END)
        for t in self._tribunal_keys:
            if term in t:
                self._trib_list.insert(tk.END, t)

    def _selected_aliases(self) -> list[str]:
        return [
            TRIBUNAIS[self._trib_list.get(i)]
            for i in self._trib_list.curselection()
        ]

    # ── helpers de aba ───────────────────────────────────────────────────────

    def _update_proc_mode(self) -> None:
        if self._proc_mode.get() == "multiplo":
            self._proc_csv_btn.state(["!disabled"])
        else:
            self._proc_csv_btn.state(["disabled"])

    def _load_proc_csv(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("Texto", "*.txt")])
        if path:
            numeros = _read_first_column(path)
            self._proc_entry.delete(0, tk.END)
            self._proc_entry.insert(0, ", ".join(numeros))

    def _load_csv(self, entry: ttk.Entry) -> None:
        path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("Texto", "*.txt")])
        if path:
            valores = _read_first_column(path)
            entry.delete(0, tk.END)
            entry.insert(0, ", ".join(valores))

    # ── construção da query a partir da aba ativa ────────────────────────────

    def _build_query(self) -> dict:
        tab_idx = self._notebook.index(self._notebook.select())
        date_gte = self._date_gte.get().strip() or None
        date_lt  = self._date_lt.get().strip() or None

        if date_gte:
            date_gte = date_gte + "T00:00:00.000Z"
        if date_lt:
            date_lt = date_lt + "T00:00:00.000Z"

        if tab_idx == 0:  # Processo
            raw = self._proc_entry.get().strip()
            if not raw:
                raise ValueError("Informe ao menos um número de processo.")
            nums = [n.strip() for n in raw.split(",") if n.strip()]
            if len(nums) == 1:
                return Q.por_numero_processo(nums[0])
            return Q.por_numeros_processo(nums)

        if tab_idx == 1:  # Classe
            raw = self._classe_entry.get().strip()
            if not raw:
                raise ValueError("Informe ao menos um código de classe.")
            codigos = [int(c.strip()) for c in raw.split(",") if c.strip()]
            if len(codigos) == 1:
                return Q.por_classe(codigos[0], date_gte=date_gte, date_lt=date_lt)
            return Q.por_classes(codigos, date_gte=date_gte, date_lt=date_lt)

        if tab_idx == 2:  # Assunto
            raw = self._assunto_entry.get().strip()
            if not raw:
                raise ValueError("Informe ao menos um código de assunto.")
            codigos = [int(c.strip()) for c in raw.split(",") if c.strip()]
            if len(codigos) == 1:
                return Q.por_assunto(codigos[0], date_gte=date_gte, date_lt=date_lt)
            return Q.por_assuntos(codigos, date_gte=date_gte, date_lt=date_lt)

        if tab_idx == 3:  # Órgão
            raw = self._orgao_entry.get().strip()
            if not raw:
                raise ValueError("Informe ao menos um código de órgão julgador.")
            codigos = [int(c.strip()) for c in raw.split(",") if c.strip()]
            if len(codigos) == 1:
                return Q.por_orgao(codigos[0], date_gte=date_gte, date_lt=date_lt)
            return Q.por_orgaos(codigos, date_gte=date_gte, date_lt=date_lt)

        if tab_idx == 4:  # Município
            raw = self._municipio_entry.get().strip()
            if not raw:
                raise ValueError("Informe ao menos um código IBGE de município.")
            codigos = [int(c.strip()) for c in raw.split(",") if c.strip()]
            if len(codigos) == 1:
                return Q.por_municipio(codigos[0], date_gte=date_gte, date_lt=date_lt)
            return Q.por_municipios(codigos, date_gte=date_gte, date_lt=date_lt)

        if tab_idx == 5:  # Combinada
            def _ints(s: str) -> list[int]:
                return [int(x.strip()) for x in s.split(",") if x.strip()]

            def _strs(s: str) -> list[str]:
                return [x.strip() for x in s.split(",") if x.strip()]

            return Q.combinada(
                numeros=_strs(self._comb_proc.get()) or None,
                classes=_ints(self._comb_classes.get()) or None,
                assuntos=_ints(self._comb_assuntos.get()) or None,
                orgaos=_ints(self._comb_orgaos.get()) or None,
                municipios=_ints(self._comb_municipios.get()) or None,
                date_gte=date_gte,
                date_lt=date_lt,
            )

        raise ValueError(f"Aba desconhecida: {tab_idx}")

    # ── ações dos botões ─────────────────────────────────────────────────────

    def _executar(self) -> None:
        try:
            query_body = self._build_query()
        except (ValueError, Exception) as exc:
            messagebox.showerror("Erro na query", str(exc))
            return

        aliases = self._selected_aliases()
        if not aliases:
            messagebox.showwarning("Tribunais", "Selecione ao menos um tribunal.")
            return

        def _run() -> None:
            logging.info("Iniciando coleta para %d tribunal(is)...", len(aliases))
            try:
                ingestor.coletar_multiplos(
                    tribunal_aliases=aliases,
                    query_body=query_body,
                    page_size=int(self._page_size.get()),
                    progress_cb=lambda m: logging.info(m),
                )
                logging.info("Coleta finalizada. Execute PARSEAR para gerar os Parquets.")
            except Exception as exc:
                logging.error("Erro na coleta: %s", exc)

        threading.Thread(target=_run, daemon=True).start()

    def _parsear(self) -> None:
        def _run() -> None:
            try:
                resultado = datajud_parser.parsear(
                    raw_dir=RAW_DIR,
                    out_dir=PARSED_DIR,
                    progress_cb=lambda m: logging.info(m),
                )
                for k, p in resultado.items():
                    logging.info("Parquet gerado: %s → %s", k, p)
                self._enable_after_parse = True  # poll loop habilita botões
            except Exception as exc:
                logging.error("Erro no parse: %s", exc)

        threading.Thread(target=_run, daemon=True).start()

    def _enriquecer_tpu(self) -> None:
        self._btn_tpu.state(["disabled"])

        def _run() -> None:
            try:
                resultado = datajud_tpu.enriquecer(
                    parsed_dir=PARSED_DIR,
                    progress_cb=lambda m: logging.info(m),
                )
                for k, p in resultado.items():
                    logging.info("Parquet TPU enriquecido: %s → %s", k, p)
            except Exception as exc:
                logging.error("Erro no enriquecimento TPU: %s", exc)
            finally:
                self._enable_after_parse = True

        threading.Thread(target=_run, daemon=True).start()

    def _classificar_movimentos(self) -> None:
        self._btn_class.state(["disabled"])

        def _run() -> None:
            try:
                out = datajud_tpu.classificar_movimentos(
                    parsed_dir=PARSED_DIR,
                    progress_cb=lambda m: logging.info(m),
                )
                logging.info("Movimentos classificados → %s", out)
            except Exception as exc:
                logging.error("Erro na classificação: %s", exc)
            finally:
                self._enable_after_parse = True

        threading.Thread(target=_run, daemon=True).start()

    def _baixar_tpu_completa(self) -> None:
        def _run() -> None:
            try:
                resultado = datajud_tpu.baixar_completa(
                    progress_cb=lambda m: logging.info(m),
                )
                for k, p in resultado.items():
                    logging.info("TPU completa: %s → %s", k, p)
            except Exception as exc:
                logging.error("Erro ao baixar TPU completa: %s", exc)

        threading.Thread(target=_run, daemon=True).start()

    def _baixar_magistrados_tjpr(self) -> None:
        def _run() -> None:
            try:
                resultado = datajud_magistrados.baixar(
                    out_dir=PARSED_DIR,
                    progress_cb=lambda m: logging.info(m),
                )
                for k, p in resultado.items():
                    logging.info("Magistrados TJPR: %s → %s", k, p)
            except Exception as exc:
                logging.error("Erro ao baixar magistrados TJPR: %s", exc)

        threading.Thread(target=_run, daemon=True).start()

    def _limpar_log(self) -> None:
        self._log_text.config(state=tk.NORMAL)
        self._log_text.delete("1.0", tk.END)
        self._log_text.config(state=tk.DISABLED)

    def _abrir_data(self) -> None:
        import subprocess
        subprocess.Popen(["xdg-open", str(PARSED_DIR)])

    def _abrir_ibge_municipios(self) -> None:
        import webbrowser
        webbrowser.open("https://www.ibge.gov.br/explica/codigos-dos-municipios.php")

    # ── polling do log ────────────────────────────────────────────────────────

    def _poll_log(self) -> None:
        while not self._log_queue.empty():
            msg = self._log_queue.get_nowait()
            self._log_text.config(state=tk.NORMAL)
            self._log_text.insert(tk.END, msg + "\n")
            self._log_text.see(tk.END)
            self._log_text.config(state=tk.DISABLED)

        # habilita botões pós-parse quando flag setado por thread de background
        if self._enable_after_parse:
            self._enable_after_parse = False
            for btn in self._btns_post_parse:
                btn.state(["!disabled"])

        self.after(150, self._poll_log)


# ── utilidades ────────────────────────────────────────────────────────────────

def _read_first_column(path: str) -> list[str]:
    """Lê a primeira coluna de um CSV/TXT (ignora cabeçalho se não numérico)."""
    values: list[str] = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            val = row[0].strip()
            if i == 0 and not val.replace("-", "").isdigit():
                continue  # pula cabeçalho
            if val:
                values.append(val)
    return values
