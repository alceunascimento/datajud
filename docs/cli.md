# CLI — DataJud Query Tool

O app funciona tanto via GUI (Tkinter) quanto via linha de comando — sem display, sem Tkinter importado.

**Regra de despacho em `main.py`:**
- `python main.py` → GUI
- `python main.py <qualquer argumento>` → CLI

---

## Referência de comandos

```
python main.py --help
python main.py <comando> --help
```

### `coletar`

Coleta dados da API DataJud e salva NDJSON em `data/raw/`.

```
python main.py coletar --tipo TIPO [opções] --tribunais TRB [TRB ...]
```

**`--tipo`** (obrigatório):

| Tipo | Argumento de entrada | Aceita CSV |
|------|---------------------|------------|
| `processo` | `--numeros N1 N2 ...` | sim |
| `classe` | `--codigos C1 C2 ...` | sim |
| `assunto` | `--codigos C1 C2 ...` | sim |
| `orgao` | `--codigos C1 C2 ...` | sim |
| `combinada` | `--classes`, `--assuntos`, `--orgaos`, `--numeros` (qualquer combinação) | `--csv` → numeros |

**`--tribunais`** (obrigatório): siglas em maiúscula ou minúscula, ou aliases diretos.
```
--tribunais TJPR          # sigla maiúscula
--tribunais tjpr          # sigla minúscula
--tribunais TRF1 TRF2     # múltiplos
--tribunais trt1          # alias direto
```

**`--de` / `--ate`** (opcional): filtro de `dataAjuizamento` no formato `YYYY-MM-DD`.

**`--csv`** (opcional): arquivo CSV com códigos ou números na 1ª coluna. Ignora cabeçalho não numérico. Substitui `--codigos` / `--numeros`.

### `parsear`

Converte todos os NDJSON em `data/raw/` para 3 Parquets em `data/parsed/`.

```
python main.py parsear
```

### `enriquecer`

Enriquece os Parquets mais recentes com colunas da API TPU (LEFT JOIN por código).

```
python main.py enriquecer
```

### `classificar`

Classifica movimentos com 9 booleanos pela árvore hierárquica TPU.

```
python main.py classificar
```

### `baixar-tpu`

Baixa as tabelas TPU completas (classes, assuntos, movimentos) para `data/`.

```
python main.py baixar-tpu
```

---

## Exemplos práticos

```bash
PYTHON=~/.local/share/venvs/datajud/venv/bin/python
APP=~/code/datajud/main.py

# processo único no TRF1
$PYTHON $APP coletar --tipo processo \
    --numeros 00008323520184013202 \
    --tribunais TRF1

# classe 436 no TJPR e TJSP, ano de 2024
$PYTHON $APP coletar --tipo classe \
    --codigos 436 \
    --tribunais TJPR TJSP \
    --de 2024-01-01 --ate 2025-01-01

# múltiplas classes via CSV
$PYTHON $APP coletar --tipo classe \
    --csv ~/classes_alvo.csv \
    --tribunais TJPR

# query combinada: classe + assunto + período
$PYTHON $APP coletar --tipo combinada \
    --classes 436 \
    --assuntos 6177 \
    --tribunais TJPR \
    --de 2023-01-01

# pipeline completo em sequência
$PYTHON $APP coletar --tipo classe --codigos 436 --tribunais TJPR
$PYTHON $APP parsear
$PYTHON $APP enriquecer
$PYTHON $APP classificar

# baixa TPU antes de tudo (opcional, reutilizada nas etapas seguintes)
$PYTHON $APP baixar-tpu
```

---

## Execução em background via SSH

### nohup (mais simples)

```bash
PYTHON=~/.local/share/venvs/datajud/venv/bin/python
APP=~/code/datajud/main.py
LOG=~/code/datajud/logs/run_$(date +%Y%m%d%H%M%S).log

nohup $PYTHON $APP coletar --tipo classe --codigos 436 --tribunais TJPR \
    > $LOG 2>&1 &

echo "Rodando em background. PID: $!  Log: $LOG"

# acompanha o log em tempo real
tail -f $LOG
```

O processo continua mesmo após desconectar o SSH.

### tmux (recomendado para sessões longas)

```bash
# cria sessão e lança o processo
tmux new-session -d -s datajud \
    '~/.local/share/venvs/datajud/venv/bin/python ~/code/datajud/main.py \
     coletar --tipo classe --codigos 436 --tribunais TJPR'

# re-anexa para ver o progresso (pode desconectar com Ctrl+B D a qualquer momento)
tmux attach -t datajud

# lista sessões ativas
tmux ls

# mata a sessão quando terminar
tmux kill-session -t datajud
```

### Pipeline completo em background

```bash
PYTHON=~/.local/share/venvs/datajud/venv/bin/python
APP=~/code/datajud/main.py

tmux new-session -d -s datajud-pipeline "
  set -e
  cd ~/code/datajud
  $PYTHON $APP coletar --tipo classe --codigos 436 --tribunais TJPR TJSP && \
  $PYTHON $APP parsear && \
  $PYTHON $APP enriquecer && \
  $PYTHON $APP classificar && \
  echo 'Pipeline concluído.'
"

tmux attach -t datajud-pipeline
```

---

## Saída no terminal

```
17:04:18 INFO     [TJPR] Iniciando coleta...
17:04:20 INFO     [TJPR] Total estimado: 9542 processos
17:04:21 INFO     [tjpr] pág 1 → 1000 hits
17:04:21 INFO     [TJPR] pág 1 → 1000 registros → datajud_tjpr_20260415170418_p0001.ndjson
17:04:22 INFO     [tjpr] pág 2 → 1000 hits
...
17:08:45 INFO     [TJPR] Coleta concluída: 10 página(s), 10 arquivo(s).
```

Tudo vai para `stdout` e para `logs/datajud.log` simultaneamente.
