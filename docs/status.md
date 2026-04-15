# Status do Projeto — DataJud Query Tool

**Data:** 2026-04-15
**Versão:** 0.2.0 (funcional, em uso)

## Estado atual

### Funcionando
- Coleta de dados via API DataJud para qualquer tribunal (91 disponíveis)
- Todos os tipos de query: processo, classe, assunto, órgão julgador, combinada
- Paginação `search_after` estável com throttling e backoff
- Normalização de datas (7 formatos distintos do DataJud)
- Parse NDJSON → 3 Parquets via DuckDB (com UNNEST de arrays)
- Enriquecimento com API TPU (classes, assuntos, movimentos)
- Download da TPU completa para lookup independente
- **Classificação de movimentos** por posição na árvore hierárquica TPU (9 categorias booleanas)
- GUI Tkinter com log em tempo real, filtro de tribunais, filtro de período
- Log em arquivo (`logs/datajud.log`)

### Pipeline completo testado
```
TJPR → coleta → parse → enriquece TPU → classifica movimentos  ✓
```

## Bugs corrigidos (histórico da sessão)

| Bug | Causa | Fix |
|-----|-------|-----|
| HTTP 400 na query | Sort por `_id` sem `doc_values` no índice DataJud | Removido `_id` do sort |
| 400 sendo retried | `raise_for_status()` dentro do loop de retry | 4xx falha imediato sem retry |
| Logs duplicados | `_log()` chamava `log.info()` + `progress_cb()` (mesmo handler) | Usa um ou outro |
| Botão TPU não habilitava | `self.after(0,...)` de thread daemon não confiável no Linux | Flag booleano lido no poll loop |
| Datas 12-dígitos erradas | `strptime` com `%Y%m%d%H%M%S` aceita 1 dígito em `%M/%S` | Regex para selecionar formato |
| Movimentos sem dados TPU | Endpoint movimentos usa `id` como PK, não `cod_item` | `_registrar_tpu()` normaliza `id → cod_item` |
| URL TPU errada | Host `www.cnj.jus.br/sgt` não existe | Corrigido para `gateway.cloud.pje.jus.br/tpu` |

## Arquivos de dados

### `data/raw/`
NDJSON por página de coleta. Podem ser deletados após parse bem-sucedido.
Convenção: `datajud_{tribunal}_{ts}_p{n:04d}.ndjson`

### `data/parsed/`
| Padrão de nome | Conteúdo |
|----------------|----------|
| `processos_{ts}.parquet` | Dados brutos parseados |
| `assuntos_{ts}.parquet` | Assuntos (1 linha por assunto por processo) |
| `movimentos_{ts}.parquet` | Movimentos com complementos (1 linha por complemento) |
| `processos_{ts}_tpu.parquet` | Processos + colunas TPU de classes |
| `assuntos_{ts}_tpu.parquet` | Assuntos + colunas TPU de assuntos |
| `movimentos_{ts}_tpu.parquet` | Movimentos + colunas TPU de movimentos |
| `movimentos_{ts}_class.parquet` | Movimentos + 9 booleanos de classificação |

### `data/`
| Arquivo | Conteúdo | Tamanho |
|---------|----------|---------|
| `tpu_classes_{ts}.parquet` | Tabela TPU completa de classes | 849 itens |
| `tpu_assuntos_{ts}.parquet` | Tabela TPU completa de assuntos | 5.601 itens |
| `tpu_movimentos_{ts}.parquet` | Tabela TPU completa de movimentos | 964 itens |

## Pendências conhecidas

| Item | Prioridade | Observação |
|------|-----------|------------|
| URL TPU pode mudar | Alta | `https://gateway.cloud.pje.jus.br/tpu` — gateway PJe, não URL oficial CNJ. Monitorar. |
| Paginação sem tiebreaker | Média | Sem `_id` no sort, documentos com mesmo `@timestamp` podem ser duplicados/ignorados entre páginas. Impacto prático baixo. |
| Sem limpeza automática de `data/raw/` | Baixa | Acumula NDJSON após parse. Adicionar botão "Limpar raw". |
| `tpu_descricao_glossario` contém HTML | Info | Colunas de classes retornam HTML entities. Normalizar para texto puro se necessário para análise. |
| Classificação não combina com enriquecimento TPU | Info | `movimentos_{ts}_class.parquet` parte do parquet base, não do `_tpu`. Para ter ambos, fazer JOIN manual pelo `numero_processo + movimento_codigo`. |
