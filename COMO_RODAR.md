# Como rodar o projeto do zero

Guia passo a passo para subir o **migrate** (orquestrador de migração GCP → Databricks/AWS) em uma máquina nova.

---

## Pré-requisitos

| Requisito | Versão | Como verificar |
|---|---|---|
| **Python** | ≥ 3.11 | `python3 --version` |
| **pip** | recente | `python3 -m pip --version` |
| **git** | qualquer | `git --version` |

> O projeto exige Python 3.11 ou superior (definido em `pyproject.toml`). Se a sua versão for menor, instale uma mais nova antes de continuar.

Nenhuma credencial de nuvem é necessária para rodar em **modo de demonstração** (dados sintéticos). Credenciais só são exigidas para escanear/migrar artefatos reais.

---

## Passo 1 — Obter o código

Se ainda não tiver o repositório:

```bash
git clone https://github.com/VCasseb/orquestrador_migracao_opensource.git migrate
cd migrate
```

Se já estiver com a pasta do projeto, apenas entre nela:

```bash
cd "/Users/viniciuscasseb/Desktop/Orquestrador de Migração"
```

---

## Passo 2 — Criar e ativar o ambiente virtual

Isolar as dependências num `.venv` evita conflitos com outros projetos.

### Linux / macOS
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Windows (cmd)
```cmd
python -m venv .venv
.venv\Scripts\activate
```

Quando ativado, o prompt do terminal passa a mostrar `(.venv)` no início.

---

## Passo 3 — Instalar o pacote

```bash
pip install -e .
```

O `-e` instala em modo "editável": as mudanças no código-fonte passam a valer sem reinstalar. Esse comando também registra o executável de terminal **`migrate`** (definido em `pyproject.toml` → `[project.scripts]`).

Confirme que funcionou:

```bash
migrate --help
```

---

## Passo 4 — Inicializar o projeto

```bash
migrate init
```

Esse comando cria a pasta `.migrate/` (estado, inventário, auditoria), gera um arquivo `.env` a partir do template e garante as entradas necessárias no `.gitignore`.

---

## Passo 5 — Rodar

### Opção A — Demonstração, sem credenciais (recomendado para começar)

Carrega um inventário sintético (a cadeia medallion `cartoes`: 4 notebooks + 1 DAG orquestrador) e sobe a interface:

```bash
migrate inventory --sample
migrate web
```

### Opção B — Direto na interface web

```bash
migrate web
```

Em ambos os casos, abra no navegador:

```
http://127.0.0.1:8000
```

Comece pela aba **Connections**.

> `migrate web` é um atalho que sobe o servidor **uvicorn** apontando para o app FastAPI (`migrate.web.app:app`). Carrega o `.env` automaticamente e aplica host/porta padrão.

---

## Configuração (`.env`)

Para uso **real** (não-sample), edite o arquivo `.env` na raiz do projeto.

### GCP — onde estão os artefatos de origem
```bash
GCP_PROJECT_IDS=acme-data-prod,acme-finance-prod
GCP_COMPOSER_DAG_BUCKET=gs://us-central1-acme-composer-bucket/dags
GCP_NOTEBOOKS_BUCKET=gs://acme-data-notebooks
GCP_REGION=us-east-1
```

Autenticação no GCP (escolha uma):
- **Dev individual:** `gcloud auth application-default login` (deixe `GCP_SERVICE_ACCOUNT_JSON` vazio)
- **CI/compartilhado:** aponte `GCP_SERVICE_ACCOUNT_JSON` para um JSON de Service Account

### Provedor de IA — escolha um
```bash
LLM_PROVIDER=anthropic        # anthropic | openai | gemini | bedrock
ANTHROPIC_API_KEY=sk-ant-...
ANTHROPIC_MODEL=claude-sonnet-4-6
```

### Plataforma de destino
```bash
TARGET_PLATFORM=databricks    # ou 'aws'
TARGET_DATABRICKS_WORKSPACE_PREFIX=/Workspace/migration
TARGET_S3_NOTEBOOKS_PREFIX=s3://acme-data-notebooks/migration
TARGET_MWAA_DAGS_PREFIX=s3://acme-mwaa-bucket/dags
```

### Servidor web (opcional)
```bash
MIGRATE_WEB_HOST=127.0.0.1
MIGRATE_WEB_PORT=8000
```

---

## Resumo — do zero em 5 comandos

```bash
git clone https://github.com/VCasseb/orquestrador_migracao_opensource.git migrate
cd migrate
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
migrate init && migrate inventory --sample && migrate web
```

Depois: abra **http://127.0.0.1:8000**.

---

## Comandos úteis do CLI

```bash
migrate init                      # bootstrap .migrate/ + .env
migrate web                       # sobe a UI local
migrate inventory --sample        # inventário sintético de demonstração
migrate inventory                 # escaneia GCP real (DAGs, notebooks, tabelas)
migrate convert dag <dag_id>      # conversão de um DAG do Composer
migrate convert notebook <nb_id>  # conversão de um notebook
migrate plan sprint-1             # ondas (waves) ordenadas por dependência
migrate deploy sprint-1 --mode sample      # deploy simulado
migrate validate <fqn> --sample   # validação sintética
migrate status                    # resumo rápido
```

Rode `migrate --help` ou `migrate <comando> --help` para detalhes de cada subcomando.

---

## Problemas comuns

| Sintoma | Causa provável | Solução |
|---|---|---|
| `command not found: migrate` | venv não ativado ou `pip install -e .` não rodou | Ative o `.venv` e reinstale |
| Erro de versão do Python | Python < 3.11 | Instale Python 3.11+ e recrie o `.venv` |
| Porta 8000 ocupada | Outro processo na porta | `migrate web --port 8080` ou ajuste `MIGRATE_WEB_PORT` |
| Erros de credencial GCP/LLM | `.env` incompleto | Use `migrate inventory --sample` ou preencha o `.env` |

---

## O que NÃO é migrado

O framework migra **código** (DAGs e notebooks). **Não** move dados (BQ→Delta, GCS→S3), nem cuida de rede/IAM, dashboards de BI ou MLOps do Vertex. Tabelas são escaneadas apenas para contexto de linhagem.
