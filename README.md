# ShopBrasil – Pipeline de preços por categoria (Apache Airflow)

Pipeline em Apache Airflow que **aposenta o script em cron** que alimentava o painel de preços da equipe de pricing da ShopBrasil. Coleta o catálogo da [FakeStore API](https://fakestoreapi.com/docs), calcula as métricas por categoria (preço médio, mínimo, máximo e quantidade de produtos) e grava o resultado na base analítica em PostgreSQL, todo dia às **06:00 (horário de Brasília)**.

## O problema que resolve

| Dor do script em cron | Como o pipeline resolve |
|---|---|
| API oscila de madrugada e o script falha em silêncio | Retry com **exponential backoff** + callbacks de retry/falha + alerta |
| Rodar "na mão" duplica números | Gravação **idempotente** (DELETE + INSERT em uma transação, com chave primária) |
| Nova categoria exige editar código | **Dynamic Task Mapping**: cada categoria vira uma task sozinha |
| Código monolítico, difícil de manter | TaskFlow API, tasks pequenas, 3 TaskGroups e um operador customizado |

## Arquitetura

```
┌─ ingestao ───────────────────────────┐   ┌─ analise ──────────────────────────────────────┐   ┌─ persistencia ─────┐
│ buscar_produtos ─► validar_produtos  │──►│ agrupar_por_categoria ─► calcular_metricas[0..N]│──►│ salvar_metricas    │
│   (retry + backoff + callbacks)      │   │                          (fan-out, pool 2 slots)│   │ (idempotente)      │
│                                      │   │                                     └─► consolidar (fan-in)            │
└──────────────────────────────────────┘   └────────────────────────────────────────────────┘   └────────────────────┘
        LINEAR                                        FAN-OUT  ─────────────►  FAN-IN
```

- **Linear:** `buscar_produtos → validar_produtos → agrupar_por_categoria`
- **Fan-out:** `calcular_metricas.expand(grupo=...)`, uma task por categoria
- **Fan-in:** `consolidar` junta os resultados de todas as categorias
- **Persistência:** `salvar_metricas` grava `metricas_categoria` (snapshot) e `historico_metricas_categoria` (append)

Containers (`docker-compose.yml`): `postgres-meta` (estado do Airflow), `postgres-dw` (base analítica), `airflow-init` (migra, cria usuário, Connection e Pool), `airflow-scheduler` e `airflow-webserver`. Executor: **LocalExecutor**.

## Como executar do zero

Pré-requisitos: Docker Desktop (ou Docker Engine + Compose v2) e ~4 GB de RAM livres. Acesso à internet (a API é pública).

```bash
git clone <URL-DO-REPOSITORIO> && cd atividade-01-airflow
mkdir -p logs && chmod -R 777 logs      # só no Linux
docker compose up -d
docker compose ps                        # aguarde "healthy" no scheduler e no webserver
```

- UI do Airflow: <http://localhost:8080> · usuário `admin` · senha `admin`
- Banco analítico: `localhost:5434` · usuário `shop` · senha `shop123` · base `shopbrasil_dw`

A DAG `shopbrasil_precos_por_categoria` já nasce **ligada**. Ao subir, o Airflow executa sozinho a última janela perdida (`catchup=False`) e depois segue o agendamento das 06:00. Para disparar manualmente: botão ▶ **Trigger DAG** na UI ou

```bash
docker compose exec airflow-scheduler airflow dags trigger shopbrasil_precos_por_categoria
```

### Conferir o resultado

```bash
docker exec shopbrasil-dw psql -U shop -d shopbrasil_dw -c "SELECT * FROM v_painel_precos;"
docker exec shopbrasil-dw psql -U shop -d shopbrasil_dw -c "SELECT run_id, data_execucao, count(*) FROM historico_metricas_categoria GROUP BY 1,2 ORDER BY 1;"
```

### Demonstrar a resiliência (falha provocada)

Aponte a API para uma rota inválida e reduza a espera do retry para ver tudo em ~1 minuto:

```bash
FAKESTORE_BASE_URL=https://fakestoreapi.com/rota-invalida RETRY_DELAY_SECONDS=5 docker compose up -d
docker compose exec airflow-scheduler airflow dags trigger shopbrasil_precos_por_categoria
# na UI: buscar_produtos fica laranja (retry), depois vermelha; o log mostra 🔁 RETRY e 🚨 ALERTA
docker compose up -d      # volta à configuração normal (API real, espera de 60 s)
```

### Demonstrar a idempotência

1. Dispare a DAG duas vezes no mesmo dia (ou limpe/reexecute uma run em *Clear*).
2. `SELECT count(*) FROM metricas_categoria;` continua igual ao número de categorias (4 com a FakeStore API).
3. `historico_metricas_categoria` ganha linhas **só quando existe uma run nova**; reprocessar a mesma run não duplica.

### Derrubar

```bash
docker compose down        # mantém os dados
docker compose down -v     # apaga bancos e recomeça do zero
```

## Requisitos da atividade → onde estão

| Requisito | Onde |
|---|---|
| TaskFlow API (`@dag`/`@task`), dependências pela chamada das funções | `dags/shopbrasil_precos_por_categoria.py` |
| XComs via `return`, só dados pequenos | `buscar_produtos` devolve apenas `id, title, price, category`; cada task mapeada recebe só os preços da sua categoria |
| Topologias linear, fan-out e fan-in identificáveis | diagrama acima; `.expand()` e `consolidar` |
| Timezone `America/Sao_Paulo` (pendulum), `start_date`, `catchup=False` | `@dag(schedule="0 6 * * *", start_date=pendulum.datetime(2024,1,1,tz=...), catchup=False)` |
| `buscar_produtos` com retry e exponential backoff | `retries=4`, `retry_exponential_backoff=True`, `max_retry_delay=15 min` |
| `try/except` + `raise` | `buscar_produtos` (log do erro e `raise` para acionar o retry) |
| Callbacks `on_failure`, `on_retry`, `on_success` na task crítica | `buscar_produtos` |
| Dynamic Task Mapping | `calcular_metricas.expand(grupo=grupos)` |
| Pool `ecommerce_pool` com 2 slots | criado pelo `airflow-init`; usado em `calcular_metricas` |
| Pelo menos 2 TaskGroups | `ingestao`, `analise`, `persistencia` |
| `PostgresHook` + Connection | `salvar_metricas` com a Connection `postgres_shopbrasil` (criada no `airflow-init`) |
| Gravação idempotente | DELETE + INSERT na mesma transação, `PRIMARY KEY (data_referencia, categoria)` |
| **Opcional:** operador customizado | `plugins/shopbrasil/validar_produtos.py` (`ValidarProdutosOperator`) |
| **Opcional:** tabela de histórico (append) | `historico_metricas_categoria` |
| **Opcional:** alerta de falha | `on_failure_callback` que simula o envio do alerta (default de todas as tasks) |

## Decisões técnicas

- **Retry só onde faz sentido.** A ingestão faz retry com backoff (1 min, 2, 4, 8, teto de 15 min) porque a instabilidade da API é transitória. O `ValidarProdutosOperator` usa `AirflowFailException`: schema inválido é falha **definitiva**, repetir a mesma resposta não conserta.
- **Idempotência em duas camadas.** O snapshot usa DELETE + INSERT em **uma transação** (se algo falhar, nada muda) e a PK garante que o banco rejeita duplicatas mesmo se alguém burlar o código. DELETE + INSERT (em vez de só `ON CONFLICT`) também remove categorias que deixaram de existir no dia.
- **`data_referencia`** é o dia da execução em Brasília: para run agendada usa o fim da janela (`data_interval_end`, estável mesmo se a run for reprocessada dias depois); para disparo manual usa o instante do disparo. Assim, rodar duas vezes no mesmo dia sobrescreve o snapshot em vez de duplicar.
- **Histórico em append sem duplicar:** `UNIQUE (run_id, categoria)` + `ON CONFLICT DO NOTHING`. Cada execução distinta acrescenta linhas (evolução dos preços); reprocessar a mesma run não.
- **Fan-out por categoria** em vez de lista fixa: o `agrupar_por_categoria` descobre as categorias que existem *hoje* na resposta da API, então categoria nova vira task nova sem mudar código. O pool limita a 2 tasks simultâneas.
- **XComs pequenos:** a task de busca descarta descrição e imagem dos produtos; o fan-out passa só a lista de preços da categoria.
- **Pipeline ligado por padrão** (`DAGS_ARE_PAUSED_AT_CREATION=false`) para refletir o objetivo "rodar sozinho todo dia".
- **LocalExecutor:** suficiente para o volume (4 categorias) e para o ambiente da atividade; o código não muda se o executor for trocado por Celery/Kubernetes.
- **SLA:** o enunciado aceita SLA *ou* `on_failure_callback`. Optei pelo callback, que simula o envio do alerta (Slack/e-mail em produção).
- **Credenciais do `docker-compose.yml` são de desenvolvimento local.** Em produção devem vir de secrets/variáveis de ambiente.

## Estrutura

```
atividade-01-airflow/
├── docker-compose.yml
├── dags/shopbrasil_precos_por_categoria.py   # a DAG
├── plugins/shopbrasil/validar_produtos.py    # operador customizado
├── sql/init.sql                              # tabelas + view v_painel_precos
└── README.md
```

## Solução de problemas

| Sintoma | O que fazer |
|---|---|
| `localhost:8080` não abre | `docker compose ps`; veja se o webserver está `healthy`; `docker compose logs airflow-webserver` |
| DAG não aparece | `docker compose exec airflow-scheduler airflow dags list-import-errors` |
| `PermissionError` em `logs/` (Linux) | `chmod -R 777 logs && docker compose restart` |
| Porta 8080 ou 5434 ocupada | pare o outro serviço (ex.: outro stack do Airflow) |
| Recomeçar do zero | `docker compose down -v && docker compose up -d` |
