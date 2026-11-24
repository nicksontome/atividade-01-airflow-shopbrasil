# ShopBrasil - Pipeline de preços por categoria

Pipeline em Apache Airflow que busca os produtos da [FakeStore API](https://fakestoreapi.com/docs), calcula preço médio, mínimo, máximo e quantidade de produtos por categoria e grava no PostgreSQL. Roda todo dia às 06:00 (horário de Brasília) e substitui o script em cron.

## Estrutura do pipeline

```
ingestao:      buscar_produtos -> validar_produtos
analise:       agrupar_por_categoria -> calcular_metricas (uma task por categoria) -> consolidar
persistencia:  salvar_metricas
```

- Linear: buscar -> validar -> agrupar
- Fan-out: `calcular_metricas.expand(...)`, uma task por categoria
- Fan-in: `consolidar`
- `salvar_metricas` grava duas tabelas: `metricas_categoria` (snapshot do dia) e `historico_metricas_categoria` (append)

## Como rodar

Precisa de Docker e Docker Compose.

```bash
git clone https://github.com/nicksontome/atividade-01-airflow-shopbrasil.git
cd atividade-01-airflow-shopbrasil
mkdir -p logs && chmod -R 777 logs     # só no Linux
docker compose up -d
docker compose ps                       # espere scheduler e webserver ficarem healthy
```

- Airflow: http://localhost:8080 (admin / admin)
- Banco analítico: localhost:5434, usuário `shop`, senha `shop123`, base `shopbrasil_dw`

A DAG `shopbrasil_precos_por_categoria` já sobe ligada. Para rodar na hora, use o botão Trigger DAG na UI ou:

```bash
docker compose exec airflow-scheduler airflow dags trigger shopbrasil_precos_por_categoria
```

Ver o resultado:

```bash
docker exec shopbrasil-dw psql -U shop -d shopbrasil_dw -c "SELECT * FROM v_painel_precos;"
```

Para derrubar: `docker compose down` (mantém os dados) ou `docker compose down -v` (apaga tudo).

## Testar a falha da API

```bash
FAKESTORE_BASE_URL=https://fakestoreapi.com/rota-invalida RETRY_DELAY_SECONDS=5 docker compose up -d
docker compose exec airflow-scheduler airflow dags trigger shopbrasil_precos_por_categoria
```

`buscar_produtos` tenta de novo com espera crescente e no fim falha. Os logs mostram as mensagens de RETRY e ALERTA. Para voltar ao normal, rode `docker compose up -d` sem as variáveis.

## Requisitos da atividade

| Requisito | Onde está |
|---|---|
| TaskFlow API, XCom pelo return | `dags/shopbrasil_precos_por_categoria.py` |
| Timezone America/Sao_Paulo, start_date, catchup=False | parâmetros do `@dag` |
| Retry com exponential backoff | `buscar_produtos` (`retries=4`, `retry_exponential_backoff=True`) |
| try/except + raise | `buscar_produtos` |
| Callbacks de failure, retry e success | `buscar_produtos` |
| Dynamic Task Mapping | `calcular_metricas.expand(...)` |
| Pool `ecommerce_pool` com 2 slots | criado no `airflow-init`, usado em `calcular_metricas` |
| Pelo menos 2 TaskGroups | `ingestao`, `analise` e `persistencia` |
| PostgresHook e Connection | `salvar_metricas`, connection `postgres_shopbrasil` |
| Gravação idempotente | DELETE + INSERT na mesma transação, com chave primária |
| Opcional: operador customizado | `plugins/shopbrasil/validar_produtos.py` |
| Opcional: tabela de histórico | `historico_metricas_categoria` |
| Opcional: alerta de falha | `on_failure_callback` (simula o envio do alerta) |

## Decisões

- **Idempotência:** o snapshot apaga as linhas do dia e insere de novo, tudo numa transação. A chave primária (data, categoria) impede duplicados. No histórico, `UNIQUE (run_id, categoria)` com `ON CONFLICT DO NOTHING` evita duplicar ao reprocessar a mesma run.
- **Data de referência:** em run agendada usa o fim da janela (`data_interval_end`); em disparo manual usa o `logical_date`. Em ambos o dia é o de Brasília.
- **Validação:** o `ValidarProdutosOperator` usa `AirflowFailException`, então schema inválido falha sem retry.
- **Categorias novas:** são descobertas na resposta da API, então viram tasks sozinhas.
- **Alerta:** usei `on_failure_callback` em vez de SLA.
- **Credenciais** do `docker-compose.yml` são só para ambiente local.

## Arquivos

```
docker-compose.yml
dags/shopbrasil_precos_por_categoria.py
plugins/shopbrasil/validar_produtos.py
sql/init.sql
```

## Problemas comuns

| Problema | O que fazer |
|---|---|
| `localhost:8080` não abre | `docker compose ps` e `docker compose logs airflow-webserver` |
| DAG não aparece | `docker compose exec airflow-scheduler airflow dags list-import-errors` |
| `PermissionError` em `logs/` | `chmod -R 777 logs && docker compose restart` |
| Porta 8080 ou 5434 ocupada | pare o outro serviço que usa a porta |
