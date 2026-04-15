# DataJud : app de consultas

App desktop Python (Linux) para consultar a [API Pública do DataJud (CNJ)](https://datajud-wiki.cnj.jus.br/api-publica/), parsear os resultados em Parquet, enriquecê-los com as [Tabelas Processuais Unificadas (TPU)](https://gateway.cloud.pje.jus.br/tpu) e classificar movimentos pela hierarquia TPU.

## Requisitos

- Linux
- Python 3.12
- Virtualenv em `~/.local/share/venvs/datajud/venv/`

## Instalação

```bash
~/.local/share/venvs/datajud/venv/bin/pip install -r requirements.txt
```

## Uso

### GUI (desktop)
```bash
~/.local/share/venvs/datajud/venv/bin/python main.py
```

### CLI (terminal / SSH sem display)
```bash
PYTHON=~/.local/share/venvs/datajud/venv/bin/python

$PYTHON main.py --help
$PYTHON main.py coletar --tipo classe --codigos 436 --tribunais TJPR
$PYTHON main.py parsear
$PYTHON main.py enriquecer
$PYTHON main.py classificar
$PYTHON main.py baixar-tpu
```

**Em background via SSH:**
```bash
nohup $PYTHON main.py coletar --tipo classe --codigos 436 --tribunais TJPR \
    > logs/run.log 2>&1 &

# ou com tmux (recomendado para pipelines longos)
tmux new-session -d -s datajud "$PYTHON main.py coletar ..."
tmux attach -t datajud
```

Ver `docs/cli.md` para referência completa com todos os exemplos.

## Fluxo de trabalho

```
1. Escolhe aba de query
2. Preenche parâmetros (código, número, CSV)
3. Seleciona tribunais + período (opcional)
4. ▶ EXECUTAR QUERY      → coleta NDJSON em data/raw/
5. ⚙ PARSEAR NDJSON      → gera 3 Parquets em data/parsed/
6. ★ ENRIQUECER TPU      → adiciona colunas TPU nos Parquets   (habilitado após parse)
   ⚡ CLASSIFICAR MOV.   → classifica movimentos pela árvore   (habilitado após parse)
   ⬇ BAIXAR TPU COMPLETA → salva tabelas TPU brutas em data/  (sempre disponível)
```

## Tipos de query

| Aba | Campo ES | Único | Múltiplo |
|-----|----------|-------|----------|
| Processo | `numeroProcesso` | número digitado | vírgula ou CSV |
| Classe | `classe.codigo` (TPU) | código digitado | vírgula ou CSV |
| Assunto | `assuntos.codigo` (nested) | código digitado | vírgula ou CSV |
| Órgão Julgador | `orgaoJulgador.codigo` | código digitado | vírgula ou CSV |
| Combinada | qualquer combinação | — | — |

Todas as abas aceitam filtro de **período de ajuizamento** e rodam em **um ou múltiplos tribunais**.

## Saída

```
data/
├── raw/
│   └── datajud_{tribunal}_{ts}_p{n}.ndjson      # páginas brutas (descartável após parse)
├── parsed/
│   ├── processos_{ts}.parquet
│   ├── assuntos_{ts}.parquet
│   ├── movimentos_{ts}.parquet
│   ├── processos_{ts}_tpu.parquet                # + colunas TPU
│   ├── assuntos_{ts}_tpu.parquet
│   ├── movimentos_{ts}_tpu.parquet
│   └── movimentos_{ts}_class.parquet             # + 9 booleanos de classificação
├── tpu_classes_{ts}.parquet                      # TPU completa (849 classes)
├── tpu_assuntos_{ts}.parquet                     # TPU completa (5.601 assuntos)
└── tpu_movimentos_{ts}.parquet                   # TPU completa (964 movimentos)
```

> [!CAUTION]
> Os dados de tempo do DATAJUD não são confiáveis. Há múltiplos formatos na base de dados (ISO, 14 digitos, etc.).

> [!WARNING]
> Como o CNJ definiu que o numero processual único se mantém na primeira instância, nas apelações e nos cumprimentos de sentença, não há como individualizar os processos somente pelo _`numero_processo`, é preciso utilizar a variável `id` que é uma `text/keyword` que, pelo Glossário do DATAJUD é : "Identificador da origem do processo no Datajud - Chave Tribunal_Classe_Grau_OrgaoJulgador_NumeroProcesso". Com isto, é possível individualizar um mesmo processo como "TJPR_G1_00000000000000" e "TJPR_G2_000000000000000". Contudo, é raro que o dado tenha conformidade com o padrão do CNJ, na maioria dos casos existe apenas o "Tribunal" o "Grau" e o "NumeroProceso".

> [!IMPORTANT]
> Os dados de texto longo não são higienizados pelo DATAJUD, há muito ruído de encoding neles.

## Schemas

**Chave primária:** `id` (vem do `_id` do Elasticsearch DataJud). Em paralelo, o pipeline gera `id_local` reconstruído no padrão canônico `{tribunal}_{classe_codigo}_{grau}_{orgao_julgador_codigo}_{numero_processo}` (com `NA` onde faltar). Ambos os campos estão em **todos os parquets** (processos, assuntos, movimentos, versões `_tpu` e `_class`). Ver WARNING acima sobre por que `numero_processo` não serve como PK.

### processos

| Coluna | Tipo |
|--------|------|
| `id` | VARCHAR (PK) |
| `id_local` | VARCHAR |
| `numero_processo` | VARCHAR |
| `classe_codigo / nome` | INTEGER / VARCHAR |
| `sistema_codigo / nome` | INTEGER / VARCHAR |
| `formato_codigo / nome` | INTEGER / VARCHAR |
| `tribunal` | VARCHAR |
| `grau` | VARCHAR |
| `nivel_sigilo` | INTEGER |
| `data_ajuizamento` | TIMESTAMP |
| `orgao_julgador_codigo / nome` | INTEGER / VARCHAR |
| `orgao_municipio_ibge` | VARCHAR |
| `ts_index` | TIMESTAMP |

### assuntos

| Coluna | Tipo |
|--------|------|
| `id` | VARCHAR (FK → processos.id) |
| `id_local` | VARCHAR |
| `numero_processo` | VARCHAR |
| `assunto_codigo` | INTEGER |
| `assunto_nome` | VARCHAR |

### movimentos

| Coluna | Tipo |
|--------|------|
| `id` | VARCHAR (FK → processos.id) |
| `id_local` | VARCHAR |
| `numero_processo` | VARCHAR |
| `movimento_codigo` | INTEGER |
| `movimento_nome` | VARCHAR |
| `movimento_data_hora` | TIMESTAMP |
| `complemento_codigo / nome / valor / descricao` | INTEGER / VARCHAR |

### Colunas TPU (`*_tpu.parquet`)

| Coluna | Disponível em |
|--------|---------------|
| `tpu_cod_item_pai` | todos |
| `tpu_nome` | todos |
| `tpu_natureza` | processos, assuntos |
| `tpu_sigla` | processos, assuntos |
| `tpu_descricao_glossario` | todos |
| `tpu_norma` | todos |
| `tpu_situacao` | todos (`A`=ativo, `I`=inativo) |

### Classificação de movimentos (`movimentos_{ts}_class.parquet`)

Todas as colunas de `movimentos_{ts}.parquet` mais 9 booleanos baseados na posição do movimento na árvore hierárquica TPU:

```
1  Magistrado
├── 3    → decisao
├── 11009 → despacho
└── 193  Julgamento
    ├── 385 → julgamento_com_resolucao_do_merito
    └── 218 → julgamento_sem_resolucao_do_merito

14 Serventuário
├── 48  → escrivao
└── 104 Oficial de Justiça
    ├── 105 Devolução
    │   └── 106 → oficial_justica_devolucao_mandado
    └── 115 Recebimento
        └── 985 → oficial_justica_recebimento_mandado
```

| Coluna | Tipo |
|--------|------|
| `magistrado` | BOOLEAN |
| `serventuario` | BOOLEAN |
| `escrivao` | BOOLEAN |
| `decisao` | BOOLEAN |
| `despacho` | BOOLEAN |
| `julgamento_com_resolucao_do_merito` | BOOLEAN |
| `julgamento_sem_resolucao_do_merito` | BOOLEAN |
| `oficial_justica_devolucao_mandado` | BOOLEAN |
| `oficial_justica_recebimento_mandado` | BOOLEAN |

Um movimento recebe `True` se ele próprio **ou qualquer ancestral seu** na árvore tiver o código-âncora da categoria.

## Estrutura do código

```
datajud/
├── main.py          # entrypoint (CLI vs GUI) + logging para arquivo
├── cli.py           # argparse → orquestra ingestor/parser/tpu
├── gui.py           # Tkinter GUI + thread safety via poll loop
├── config.py        # constantes, paths, 91 tribunais, URLs das APIs
├── api.py           # HTTP client DataJud: search_after, backoff exponencial
├── query.py         # builders DSL Elasticsearch
├── ingestor.py      # coleta + id/id_local + normalização de datas + flush NDJSON
├── parser.py        # DuckDB: NDJSON → 3 Parquets (com UNNEST)
├── tpu.py           # facade: reexporta enriquecer/baixar_completa/classificar
├── tpu_client.py    # HTTP GET na API TPU (PJe Gateway)
├── tpu_download.py  # dump cru das 3 tabelas TPU em Parquet
├── tpu_enrich.py    # LEFT JOIN parquets × TPU → *_tpu.parquet
├── tpu_classify.py  # 9 booleanos via árvore hierárquica → *_class.parquet
├── requirements.txt
├── docs/
│   ├── arquitetura.md   # documentação técnica completa
│   ├── cli.md           # referência da linha de comando
│   └── status.md        # estado atual, bugs corrigidos, pendências
├── data/
│   ├── raw/
│   └── parsed/
└── logs/
    └── datajud.log
```

## Detalhes técnicos

**Paginação:** `search_after` com sort por `@timestamp`. Sem `_id` — o índice DataJud não tem `doc_values` nesse campo e retorna HTTP 400.

**Memória:** cada página é descarregada em NDJSON imediatamente. Parse via DuckDB com `UNNEST` SQL — sem explosão em Python.

**Datas:** 7 formatos distintos (ISO com/sem Z, numérico 8/12/14 dígitos). Normalizadas no ingestor; macro DuckDB de fallback no parser.

**Throttling:** 500ms entre páginas + backoff `2^n` em 429/503. 4xx falham imediatamente.

**Thread safety GUI:** log via `queue.Queue` + poll loop a 150ms. Botões pós-parse habilitados via flag booleano lido no main thread — `self._btns_post_parse = [btn_tpu, btn_class]`.

**Classificação de movimentos:** dict `{id → pai}` construído da TPU + cache memoizado de ancestrais. O(n × depth), depth ≤ 6. Reutiliza `tpu_movimentos_*.parquet` se já baixado.

## APIs utilizadas

| API | URL | Auth |
|-----|-----|------|
| DataJud | `https://api-publica.datajud.cnj.jus.br/api_publica_{tribunal}/_search` | APIKey pública CNJ |
| TPU | `https://gateway.cloud.pje.jus.br/tpu/api/v1/publico/download/{tipo}` | Nenhuma |

**91 tribunais:** STJ, TST, TSE, STM, TRF1–6, TRT1–24, TJ de todos os estados + TJDFT, TRE de todos os estados, TJMMG/TJMRS/TJMSP.
