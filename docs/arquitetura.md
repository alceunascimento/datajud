# Arquitetura — DataJud Query Tool

## Visão geral

App desktop Python (Linux) para extrair dados processuais da API Pública do DataJud (CNJ), parsear em Parquet, enriquecer com as Tabelas Processuais Unificadas (TPU) e classificar movimentos pela árvore hierárquica TPU.

```
┌──────────────────────────────────────────────────────┐
│                     GUI (Tkinter)                    │
│  Abas: Processo | Classe | Assunto | Órgão | Combinada
│  Painel direito: Tribunais (filtro + multiselect) + Datas
│                                                      │
│  Linha 1: ▶ EXECUTAR  ⚙ PARSEAR  ★ ENRIQUECER TPU  ⚡ CLASSIFICAR MOV.
│  Linha 2: ⬇ BAIXAR TPU COMPLETA  🗑 LIMPAR LOG  📂 Abrir data/
└──────────────┬───────────────────────────────────────┘
               │ threads daemon
   ┌───────────▼────────────┐
   │     ingestor.py        │
   │  coletar /             │
   │  coletar_multiplos     │
   └───────────┬────────────┘
               │ usa
   ┌───────────▼────────────┐
   │       api.py           │◄── query.py (DSL builders)
   │  count / search        │
   │  _post + backoff       │
   └───────────┬────────────┘
               │ NDJSON pages → data/raw/
   ┌───────────▼────────────┐
   │      parser.py         │
   │  DuckDB: NDJSON →      │
   │  3 Parquets            │
   └───────────┬────────────┘
               │ data/parsed/
   ┌───────────▼────────────────────────────────────────┐
   │           subsistema TPU (4 módulos)                │
   │                                                     │
   │   tpu_client.py     — HTTP GET na API TPU           │◄── API TPU (PJe Gateway)
   │   tpu_download.py   — dump cru das 3 tabelas        │
   │   tpu_enrich.py     — LEFT JOIN → *_tpu.parquet     │
   │   tpu_classify.py   — booleanos → *_class.parquet   │
   │   tpu.py            — facade (API pública estável)  │
   └─────────────────────────────────────────────────────┘
```

## Módulos

### `main.py`
Entrypoint. Configura logging para arquivo (`logs/datajud.log`) e lança a GUI.

### `config.py`
Todas as constantes do projeto:
- Paths: `BASE_DIR`, `DATA_DIR`, `RAW_DIR`, `PARSED_DIR`, `LOGS_DIR`
- API DataJud: `API_KEY`, `BASE_URL`, `HEADERS`, `PAGE_SIZE`
- API TPU: `TPU_BASE_URL`, `TPU_ENDPOINTS`
- `TRIBUNAIS`: dict `{sigla → alias}` com 91 tribunais

### `api.py`
Cliente HTTP de baixo nível para a API DataJud.

| Função | Descrição |
|--------|-----------|
| `count(alias, body)` | Retorna total de hits sem baixar dados (`track_total_hits`) |
| `search(alias, body, page_size)` | Generator paginado via `search_after` |
| `_post(alias, body)` | POST com backoff exponencial em 429/503; 4xx falha imediato |

**Decisões de design:**
- `search_after` com sort por `@timestamp` apenas — `_id` não tem `doc_values` no índice DataJud e causa HTTP 400.
- 4xx não é retried (erro do cliente); 429/503 → backoff `2^n` até 6 tentativas.
- 500ms de sleep entre páginas (throttling defensivo).

### `query.py`
Builders de Elasticsearch DSL. Todos retornam `dict` pronto para POST.

| Função | Filtro ES | Observação |
|--------|-----------|------------|
| `por_numero_processo(n)` | `match` em `numeroProcesso` | — |
| `por_numeros_processo(ns)` | `terms` em `numeroProcesso` | — |
| `por_classe(cod)` | `match` em `classe.codigo` | aceita `date_gte/lt` |
| `por_classes(cods)` | `terms` em `classe.codigo` | aceita `date_gte/lt` |
| `por_assunto(cod)` | `nested` match em `assuntos.codigo` | campo nested — nunca usar match direto |
| `por_assuntos(cods)` | `nested` terms em `assuntos.codigo` | campo nested |
| `por_orgao(cod)` | `match` em `orgaoJulgador.codigo` | aceita `date_gte/lt` |
| `por_orgaos(cods)` | `terms` em `orgaoJulgador.codigo` | aceita `date_gte/lt` |
| `combinada(...)` | `bool/must` com qualquer combinação | mínimo 1 filtro obrigatório |

**Regra crítica:** `assuntos` e `movimentos` são campos `nested` no índice ES. `match` direto ignora a aninhação e retorna falso-positivos — sempre usar `nested` query.

### `ingestor.py`
Orquestra a coleta e persiste em disco sem explodir RAM.

**Fluxo:**
1. `api.count()` → log do total estimado
2. `api.search()` → stream de hits
3. Para cada hit: `_normalize_hit()` extrai `_id`, constrói `id_local`, normaliza datas, serializa JSON
4. A cada `page_size` hits: `_flush()` grava NDJSON em `data/raw/`
5. Ao final: flush do buffer restante

**Chave primária `id` e `id_local`:**
- `id` ← `hit["_id"]` do Elasticsearch (PK do índice DataJud). Necessário porque o `numero_processo` do CNJ se mantém estável entre instâncias — 1º grau, apelação e cumprimento compartilham o mesmo número.
- `id_local` é reconstruído pelo ingestor no padrão canônico `{tribunal}_{classe_codigo}_{grau}_{orgao_julgador_codigo}_{numero_processo}`, com `NA` onde faltar componente e `_` escapado para `-` dentro dos valores. Serve como fallback legível quando o `id` do tribunal não segue a convenção CNJ.
- Ambos são propagados para os 3 parquets no parser e preservados nos `_tpu` e `_class`.

**Normalização de datas (`_normalize_date`):**
O DataJud retorna datas em formatos inconsistentes. A função usa pares `(regex, strptime_format)` para identificar e converter todos para `YYYY-MM-DDTHH:MM:SS`. Regex é obrigatório — `strptime` aceita 1 dígito em `%M/%S` e causa parse errado em datas numéricas de 12 dígitos.

Formatos suportados:
- `2023-03-14T10:22:00.000Z` → ISO com milissegundos e Z
- `2023-03-14T10:22:00Z` → ISO com Z
- `2023-03-14T10:22:00.000` → ISO sem Z
- `2023-03-14T10:22:00` → ISO sem Z e sem ms
- `20230314102200` → 14 dígitos numéricos
- `202303141022` → 12 dígitos numéricos
- `20230314` → 8 dígitos numéricos

### `parser.py`
Converte NDJSON brutos em 3 Parquets usando DuckDB.

**Por que DuckDB:**
- Lê múltiplos arquivos NDJSON de uma vez
- `UNNEST()` de arrays (`assuntos`, `movimentos`) em SQL — sem explosão linha-a-linha em Python
- Exporta diretamente para Parquet sem passar por pandas

**Macro `parse_dt(s)`:** fallback de normalização de datas no DuckDB para NDJSON que não passaram pelo ingestor. Usa `COALESCE(TRY_STRPTIME(...), ...)` com os 4 formatos numéricos/ISO.

**Saída:** 3 arquivos `data/parsed/{tipo}_{YYYYMMDDHHMMSS}.parquet`

### Subsistema TPU (`tpu_*.py` + `tpu.py`)

O subsistema TPU está dividido em 4 módulos por responsabilidade + 1 facade. `cli.py` e `gui.py` consomem apenas via `import tpu as datajud_tpu` — a API pública é `enriquecer`, `baixar_completa`, `classificar_movimentos`.

| Módulo | Responsabilidade | API pública |
|--------|------------------|-------------|
| `tpu_client.py`   | HTTP GET na API TPU do CNJ | `baixar(tipo, progress)` |
| `tpu_download.py` | Dump cru das 3 tabelas TPU em Parquet | `baixar_completa(out_dir)` |
| `tpu_enrich.py`   | LEFT JOIN dos parquets parseados com TPU | `enriquecer(parsed_dir)`, `encontrar_parquets_base(parsed_dir)` |
| `tpu_classify.py` | Classificação por árvore hierárquica | `classificar_movimentos(parsed_dir)` |
| `tpu.py`          | Facade — reexporta as 3 funções públicas | — |

**API TPU:** `https://gateway.cloud.pje.jus.br/tpu`
- Sem parâmetros: retorna tabela completa
- `?codigo=N`: retorna item e seus descendentes na hierarquia

**Peculiaridade:** o endpoint de movimentos usa `id` como PK, não `cod_item` (usado em classes e assuntos). `tpu_enrich._registrar_tpu()` normaliza `id → cod_item` antes do JOIN.

**Reuso entre módulos:** `tpu_classify` depende de `tpu_enrich.encontrar_parquets_base()` (localização do parquet de movimentos base) e de `tpu_client.baixar()` (fallback quando não há `tpu_movimentos_*.parquet` local).

#### Classificação de movimentos

A árvore TPU de movimentos tem 6 níveis. Dois nós raiz definem os ramos principais:

```
1  Magistrado
├── 3    Decisão
├── 11009 Despacho
└── 193  Julgamento
    ├── 385  Com Resolução do Mérito
    └── 218  Sem Resolução do Mérito

14 Serventuário
├── 48   Escrivão/Diretor de Secretaria
└── 104  Oficial de Justiça
    ├── 105  Devolução
    │   └── 106  Mandado
    └── 115  Recebimento
        └── 985  Mandado
```

**Lógica:** para cada `movimento_codigo`, a função sobe a cadeia `cod_item → cod_item_pai` até a raiz e coleta todos os ancestrais (incluindo o próprio nó). Se algum ancestral corresponde a um código-âncora, a coluna booleana recebe `True`.

**Algoritmo:** dict `{id → pai}` + cache de ancestrais memoizado por nó. O(n × depth), depth ≤ 6. Usa `tpu_movimentos_*.parquet` já baixado se disponível; caso contrário, baixa da API.

Colunas geradas:

| Coluna | Código-âncora |
|--------|---------------|
| `magistrado` | 1 |
| `serventuario` | 14 |
| `escrivao` | 48 |
| `decisao` | 3 |
| `despacho` | 11009 |
| `julgamento_com_resolucao_do_merito` | 385 |
| `julgamento_sem_resolucao_do_merito` | 218 |
| `oficial_justica_devolucao_mandado` | 106 |
| `oficial_justica_recebimento_mandado` | 985 |

### `cli.py`
Interface de linha de comando — sem importar Tkinter, seguro para SSH sem display.

Estrutura: `argparse` com subcomandos → mesmas funções que a GUI chama.

| Comando | Função chamada |
|---------|----------------|
| `coletar` | `ingestor.coletar_multiplos()` |
| `parsear` | `parser.parsear()` |
| `enriquecer` | `tpu.enriquecer()` |
| `classificar` | `tpu.classificar_movimentos()` |
| `baixar-tpu` | `tpu.baixar_completa()` |

**Despacho em `main.py`:** `len(sys.argv) > 1` → CLI; caso contrário → GUI. Tkinter nunca é importado em modo CLI.

**Logging CLI:** `StreamHandler(stdout)` + `FileHandler(logs/datajud.log)` — tudo visível no terminal e persistido em arquivo.

### `gui.py`
Interface Tkinter com polling thread-safe.

**Thread safety:** operações de rede rodam em threads daemon. Comunicação com o main thread via:
- `queue.Queue` para mensagens de log (poll a cada 150ms)
- Flag `self._enable_after_parse` lido pelo poll loop, que habilita em lote todos os botões em `self._btns_post_parse` — não usa `after()` chamado de thread (não confiável no Linux)

**Botões habilitados após parse:** `★ ENRIQUECER TPU` e `⚡ CLASSIFICAR MOV.` — ambos gerenciados pela lista `_btns_post_parse`.

## Fluxo de dados completo

```
API DataJud
    │
    ▼ hits JSON (stream, page_size=1000)
ingestor._normalize_hit()            ← extrai id, constrói id_local, normaliza datas
    │
    ▼ NDJSON normalizado
data/raw/datajud_{tribunal}_{ts}_p{n:04d}.ndjson
    │
    ▼ DuckDB read_json + UNNEST + parse_dt macro
data/parsed/processos_{ts}.parquet
data/parsed/assuntos_{ts}.parquet
data/parsed/movimentos_{ts}.parquet
    │
    ├── ▼ LEFT JOIN com TPU (classes/assuntos/movimentos)
    │   data/parsed/processos_{ts}_tpu.parquet
    │   data/parsed/assuntos_{ts}_tpu.parquet
    │   data/parsed/movimentos_{ts}_tpu.parquet
    │
    └── ▼ classificação pela árvore TPU
        data/parsed/movimentos_{ts}_class.parquet

API TPU (download completo, independente)
    │
    ▼
data/tpu_classes_{ts}.parquet
data/tpu_assuntos_{ts}.parquet
data/tpu_movimentos_{ts}.parquet
```

## Schemas dos Parquets

**Chave primária em todos os parquets:** `id` (PK do DataJud, vem do `_id` do Elasticsearch). `id_local` é a versão reconstruída no padrão canônico `{tribunal}_{classe}_{grau}_{orgao}_{numero}`. `numero_processo` **não** é único entre instâncias — nunca use como chave.

### processos_{ts}.parquet

| Coluna | Tipo | Fonte |
|--------|------|-------|
| `id` | VARCHAR | `hit._id` (PK do DataJud) |
| `id_local` | VARCHAR | ingestor, padrão CNJ |
| `numero_processo` | VARCHAR | `_source.numeroProcesso` |
| `classe_codigo` | INTEGER | `_source.classe.codigo` |
| `classe_nome` | VARCHAR | `_source.classe.nome` |
| `sistema_codigo` | INTEGER | `_source.sistema.codigo` |
| `sistema_nome` | VARCHAR | `_source.sistema.nome` |
| `formato_codigo` | INTEGER | `_source.formato.codigo` |
| `formato_nome` | VARCHAR | `_source.formato.nome` |
| `tribunal` | VARCHAR | `_source.tribunal` |
| `grau` | VARCHAR | `_source.grau` |
| `nivel_sigilo` | INTEGER | `_source.nivelSigilo` |
| `data_ajuizamento` | TIMESTAMP | `_source.dataAjuizamento` (normalizado) |
| `orgao_julgador_codigo` | INTEGER | `_source.orgaoJulgador.codigo` |
| `orgao_julgador_nome` | VARCHAR | `_source.orgaoJulgador.nome` |
| `orgao_municipio_ibge` | VARCHAR | `_source.orgaoJulgador.codigoMunicipioIBGE` |
| `ts_index` | TIMESTAMP | `_source.@timestamp` |

### assuntos_{ts}.parquet

| Coluna | Tipo |
|--------|------|
| `id` | VARCHAR (FK → processos.id) |
| `id_local` | VARCHAR |
| `numero_processo` | VARCHAR |
| `assunto_codigo` | INTEGER |
| `assunto_nome` | VARCHAR |

### movimentos_{ts}.parquet

| Coluna | Tipo |
|--------|------|
| `id` | VARCHAR (FK → processos.id) |
| `id_local` | VARCHAR |
| `numero_processo` | VARCHAR |
| `movimento_codigo` | INTEGER |
| `movimento_nome` | VARCHAR |
| `movimento_data_hora` | TIMESTAMP |
| `complemento_codigo` | INTEGER |
| `complemento_nome` | VARCHAR |
| `complemento_valor` | VARCHAR |
| `complemento_descricao` | VARCHAR |

### Colunas adicionadas pelo enriquecimento TPU (`*_tpu.parquet`)

| Coluna | Tipo | Disponível em |
|--------|------|---------------|
| `tpu_cod_item_pai` | FLOAT | processos, assuntos, movimentos |
| `tpu_nome` | VARCHAR | processos, assuntos, movimentos |
| `tpu_natureza` | VARCHAR | processos, assuntos |
| `tpu_sigla` | VARCHAR | processos, assuntos |
| `tpu_descricao_glossario` | VARCHAR | processos, assuntos, movimentos |
| `tpu_norma` | VARCHAR | processos, assuntos, movimentos |
| `tpu_situacao` | VARCHAR | processos, assuntos, movimentos (`A`=ativo, `I`=inativo) |

### Colunas adicionadas pela classificação (`movimentos_{ts}_class.parquet`)

Todas as colunas de `movimentos_{ts}.parquet` mais:

| Coluna | Tipo | Verdadeiro quando |
|--------|------|-------------------|
| `magistrado` | BOOLEAN | qualquer ancestral é código 1 |
| `serventuario` | BOOLEAN | qualquer ancestral é código 14 |
| `escrivao` | BOOLEAN | qualquer ancestral é código 48 |
| `decisao` | BOOLEAN | qualquer ancestral é código 3 |
| `despacho` | BOOLEAN | qualquer ancestral é código 11009 |
| `julgamento_com_resolucao_do_merito` | BOOLEAN | qualquer ancestral é código 385 |
| `julgamento_sem_resolucao_do_merito` | BOOLEAN | qualquer ancestral é código 218 |
| `oficial_justica_devolucao_mandado` | BOOLEAN | qualquer ancestral é código 106 |
| `oficial_justica_recebimento_mandado` | BOOLEAN | qualquer ancestral é código 985 |
