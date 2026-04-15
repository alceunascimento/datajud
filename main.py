#!/usr/bin/env python3
"""
main.py — entrypoint do DataJud Query Tool.

Sem argumentos  → abre a GUI (Tkinter)
Com argumentos  → executa via CLI (sem display necessário)

CLI — uso rápido:
    python main.py --help
    python main.py coletar --tipo classe --codigos 436 --tribunais TJPR
    python main.py parsear
    python main.py enriquecer
    python main.py classificar
    python main.py baixar-tpu

Em background (SSH sem display):
    nohup python main.py coletar --tipo classe --codigos 436 --tribunais TJPR \\
        > logs/run.log 2>&1 &
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

if len(sys.argv) > 1:
    # ── modo CLI — não importa Tkinter ──────────────────────────────────────
    from cli import run_cli
    run_cli()

else:
    # ── modo GUI ─────────────────────────────────────────────────────────────
    import logging
    from config import LOGS_DIR

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(LOGS_DIR / "datajud.log", encoding="utf-8"),
        ],
    )

    from gui import App
    app = App()
    app.mainloop()
