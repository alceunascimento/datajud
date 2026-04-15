"""
query.py — builders de DSL Elasticsearch para a API DataJud.

Regras:
- movimentos e assuntos são campos nested → usar nested query, não match direto.
- sort fixo: [{"@timestamp": "asc"}, {"_id": "asc"}] — obrigatório para search_after estável.
- Todos os builders retornam um dict pronto para POST.
"""
from typing import Optional


# ── Por número de processo ────────────────────────────────────────────────────

def por_numero_processo(numero: str) -> dict:
    """Processo único pelo número CNJ (20 dígitos, sem máscara)."""
    return {"query": {"match": {"numeroProcesso": numero.strip()}}}


def por_numeros_processo(numeros: list[str]) -> dict:
    """Lista de números de processo (terms query)."""
    cleaned = [n.strip() for n in numeros if n.strip()]
    return {"query": {"terms": {"numeroProcesso": cleaned}}}


# ── Por classe processual ─────────────────────────────────────────────────────

def por_classe(codigo: int, date_gte: Optional[str] = None, date_lt: Optional[str] = None) -> dict:
    """Classe única pelo código TPU."""
    return _bool(
        must=[{"match": {"classe.codigo": codigo}}],
        date_gte=date_gte, date_lt=date_lt,
    )


def por_classes(codigos: list[int], date_gte: Optional[str] = None, date_lt: Optional[str] = None) -> dict:
    """Múltiplas classes (terms query)."""
    return _bool(
        must=[{"terms": {"classe.codigo": codigos}}],
        date_gte=date_gte, date_lt=date_lt,
    )


# ── Por assunto (nested) ──────────────────────────────────────────────────────

def por_assunto(codigo: int, date_gte: Optional[str] = None, date_lt: Optional[str] = None) -> dict:
    """Assunto único pelo código TPU (campo nested)."""
    return _bool(
        must=[_nested_match("assuntos", "assuntos.codigo", codigo)],
        date_gte=date_gte, date_lt=date_lt,
    )


def por_assuntos(codigos: list[int], date_gte: Optional[str] = None, date_lt: Optional[str] = None) -> dict:
    """Múltiplos assuntos (nested terms)."""
    return _bool(
        must=[_nested_terms("assuntos", "assuntos.codigo", codigos)],
        date_gte=date_gte, date_lt=date_lt,
    )


# ── Por órgão julgador ────────────────────────────────────────────────────────

def por_orgao(codigo: int, date_gte: Optional[str] = None, date_lt: Optional[str] = None) -> dict:
    """Órgão julgador único pelo código."""
    return _bool(
        must=[{"match": {"orgaoJulgador.codigo": codigo}}],
        date_gte=date_gte, date_lt=date_lt,
    )


def por_orgaos(codigos: list[int], date_gte: Optional[str] = None, date_lt: Optional[str] = None) -> dict:
    """Múltiplos órgãos julgadores."""
    return _bool(
        must=[{"terms": {"orgaoJulgador.codigo": codigos}}],
        date_gte=date_gte, date_lt=date_lt,
    )


# ── Query combinada ───────────────────────────────────────────────────────────

def combinada(
    numeros: Optional[list[str]] = None,
    classes: Optional[list[int]] = None,
    assuntos: Optional[list[int]] = None,
    orgaos: Optional[list[int]] = None,
    date_gte: Optional[str] = None,
    date_lt: Optional[str] = None,
) -> dict:
    """
    Combina qualquer subconjunto dos filtros acima num único bool/must.
    Pelo menos um filtro deve ser informado.
    """
    must: list[dict] = []

    if numeros:
        must.append({"terms": {"numeroProcesso": [n.strip() for n in numeros]}})

    if classes:
        must.append({"terms": {"classe.codigo": classes}})

    if assuntos:
        must.append(_nested_terms("assuntos", "assuntos.codigo", assuntos))

    if orgaos:
        must.append({"terms": {"orgaoJulgador.codigo": orgaos}})

    if not must:
        raise ValueError("combinada() exige pelo menos um filtro.")

    return _bool(must=must, date_gte=date_gte, date_lt=date_lt)


# ── helpers internos ──────────────────────────────────────────────────────────

def _bool(must: list[dict], date_gte: Optional[str], date_lt: Optional[str]) -> dict:
    if date_gte or date_lt:
        rng: dict = {}
        if date_gte:
            rng["gte"] = date_gte
        if date_lt:
            rng["lt"] = date_lt
        must.append({"range": {"dataAjuizamento": rng}})

    return {"query": {"bool": {"must": must}}}


def _nested_match(path: str, field: str, value) -> dict:
    return {"nested": {"path": path, "query": {"match": {field: value}}}}


def _nested_terms(path: str, field: str, values: list) -> dict:
    return {"nested": {"path": path, "query": {"terms": {field: values}}}}
