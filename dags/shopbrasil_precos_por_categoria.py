"""Pipeline de preços por categoria (FakeStore API -> PostgreSQL), todo dia às 06:00."""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import timedelta
from statistics import mean

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from shopbrasil.validar_produtos import ValidarProdutosOperator

log = logging.getLogger(__name__)

# Configuração (pode ser trocada por variáveis de ambiente)
TIMEZONE = "America/Sao_Paulo"
API_BASE_URL = os.getenv("FAKESTORE_BASE_URL", "https://fakestoreapi.com")
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "60"))
POSTGRES_CONN_ID = "postgres_shopbrasil"  # criada no airflow-init
POOL = "ecommerce_pool"  # 2 slots, criado no airflow-init


# Callbacks
def _identificar(context) -> str:
    ti = context["task_instance"]
    return f"dag={ti.dag_id} task={ti.task_id} run={context['run_id']}"

def on_success_callback(context):
    log.info("SUCESSO | %s | tentativa nº %s", _identificar(context), context["task_instance"].try_number)

def on_retry_callback(context):
    log.warning(
        "RETRY | %s | erro: %s | nova tentativa será agendada com backoff",
        _identificar(context),
        context.get("exception"),
    )

def on_failure_callback(context):
    """Simula o envio de um alerta."""
    log.error(
        "ALERTA (simulado) | %s | erro: %s | Notificando #time-de-dados...",
        _identificar(context),
        context.get("exception"),
    )


# DAG
@dag(
    dag_id="shopbrasil_precos_por_categoria",
    description="Preço médio/mín/máx e qtd de produtos por categoria (FakeStore API -> PostgreSQL)",
    schedule="0 6 * * *",  # 06:00 no fuso do start_date
    start_date=pendulum.datetime(2024, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "time-de-dados",
        "on_failure_callback": on_failure_callback,
    },
    tags=["shopbrasil", "atividade-01", "etl"],
    doc_md=__doc__,
)
def shopbrasil_precos_por_categoria():

    # Ingestão
    @task(
        task_id="buscar_produtos",
        retries=4,
        retry_delay=timedelta(seconds=RETRY_DELAY_SECONDS),
        retry_exponential_backoff=True,  # a espera dobra a cada falha
        max_retry_delay=timedelta(minutes=15),
        execution_timeout=timedelta(minutes=2),
        on_success_callback=on_success_callback,
        on_retry_callback=on_retry_callback,
        on_failure_callback=on_failure_callback,
    )
    def buscar_produtos() -> list[dict]:
        """Busca os produtos e devolve só os campos necessários."""
        import requests

        url = f"{API_BASE_URL}/products"
        try:
            log.info("GET %s", url)
            resposta = requests.get(url, timeout=15, headers={"User-Agent": "shopbrasil-airflow/1.0"})
            resposta.raise_for_status()
            produtos = resposta.json()
            if not isinstance(produtos, list) or not produtos:
                raise ValueError(f"Resposta inesperada da API: {type(produtos).__name__} vazio ou inválido")
            enxutos = [
                {"id": p.get("id"), "title": p.get("title"), "price": p.get("price"), "category": p.get("category")}
                for p in produtos
            ]
        except (requests.RequestException, ValueError, AttributeError) as exc:
            log.error("Falha ao buscar produtos em %s: %s", url, exc)
            raise  # o raise faz o Airflow tentar de novo

        log.info("%d produtos coletados", len(enxutos))
        return enxutos

    # Análise
    @task(task_id="agrupar_por_categoria")
    def agrupar_por_categoria(produtos: list[dict]) -> list[dict]:
        """Uma entrada por categoria que existir na resposta."""
        grupos: dict[str, list[float]] = defaultdict(list)
        for produto in produtos:
            grupos[produto["category"]].append(produto["price"])
        lista = [{"categoria": categoria, "precos": sorted(precos)} for categoria, precos in sorted(grupos.items())]
        log.info("%d categorias encontradas: %s", len(lista), [g["categoria"] for g in lista])
        return lista

    @task(task_id="calcular_metricas", pool=POOL)
    def calcular_metricas(grupo: dict) -> dict:
        """Roda uma vez por categoria."""
        precos = grupo["precos"]
        metricas = {
            "categoria": grupo["categoria"],
            "qtd_produtos": len(precos),
            "preco_medio": round(mean(precos), 2),
            "preco_min": round(min(precos), 2),
            "preco_max": round(max(precos), 2),
        }
        log.info("Métricas: %s", metricas)
        return metricas

    @task(task_id="consolidar")
    def consolidar(metricas) -> list[dict]:
        resultado = sorted(list(metricas), key=lambda m: m["categoria"])
        log.info("Consolidadas %d categorias", len(resultado))
        return resultado

    # Persistência
    @task(task_id="salvar_metricas")
    def salvar_metricas(metricas: list[dict], **context) -> dict:
        """Grava o snapshot e o histórico numa transação.

        data_referencia: em run agendada usa o fim da janela; em disparo manual usa o logical_date.
        """
        dag_run = context["dag_run"]
        instante = context["logical_date"] if dag_run.run_type == "manual" else context["data_interval_end"]
        data_ref = instante.in_timezone(TIMEZONE).date()
        run_id = context["run_id"]

        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        try:
            with conn:  # commit se der certo, rollback se der erro
                with conn.cursor() as cur:
                    # snapshot: apaga o dia e insere de novo
                    cur.execute("DELETE FROM metricas_categoria WHERE data_referencia = %s", (data_ref,))
                    apagadas = cur.rowcount
                    cur.executemany(
                        """INSERT INTO metricas_categoria
                           (data_referencia, categoria, qtd_produtos, preco_medio, preco_min, preco_max, dag_run_id)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        [
                            (data_ref, m["categoria"], m["qtd_produtos"], m["preco_medio"],
                             m["preco_min"], m["preco_max"], run_id)
                            for m in metricas
                        ],
                    )
                    # histórico: a mesma run não duplica
                    cur.executemany(
                        """INSERT INTO historico_metricas_categoria
                           (run_id, data_execucao, categoria, qtd_produtos, preco_medio, preco_min, preco_max)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (run_id, categoria) DO NOTHING""",
                        [
                            (run_id, data_ref, m["categoria"], m["qtd_produtos"], m["preco_medio"],
                             m["preco_min"], m["preco_max"])
                            for m in metricas
                        ],
                    )
        except Exception as exc:
            log.error("Erro ao gravar no PostgreSQL (rollback feito): %s", exc)
            raise
        finally:
            conn.close()

        log.info("Snapshot de %s: %d removidas, %d inseridas | run=%s", data_ref, apagadas, len(metricas), run_id)
        return {"data_referencia": str(data_ref), "categorias": len(metricas)}

    # Encadeamento
    with TaskGroup("ingestao"):
        produtos = buscar_produtos()
        produtos_validos = ValidarProdutosOperator(task_id="validar_produtos", produtos=produtos).output

    with TaskGroup("analise"):
        grupos = agrupar_por_categoria(produtos_validos)
        metricas = calcular_metricas.expand(grupo=grupos)  # fan-out
        consolidado = consolidar(metricas)

    with TaskGroup("persistencia"):
        salvar_metricas(consolidado)

shopbrasil_precos_por_categoria()
