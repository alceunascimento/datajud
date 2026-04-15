"""
tpu.py — facade do subsistema TPU.

A implementação está dividida por responsabilidade:
  - tpu_client.py    → cliente HTTP da API TPU
  - tpu_download.py  → baixa_completa(): dump das 3 tabelas TPU em Parquet
  - tpu_enrich.py    → enriquecer(): LEFT JOIN dos parquets com TPU
  - tpu_classify.py  → classificar_movimentos(): booleanos da árvore TPU

Este módulo existe apenas para manter a API pública estável para cli.py e gui.py.
"""
from tpu_classify import classificar_movimentos
from tpu_download import baixar_completa
from tpu_enrich import enriquecer

__all__ = ["enriquecer", "baixar_completa", "classificar_movimentos"]
