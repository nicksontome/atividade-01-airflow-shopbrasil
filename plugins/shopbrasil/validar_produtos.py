"""Operador que valida o schema dos produtos antes do processamento."""

from __future__ import annotations

import logging
from typing import Any, Sequence

from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import BaseOperator

log = logging.getLogger(__name__)


class ValidarProdutosOperator(BaseOperator):
    """Valida a lista de produtos.

    Cada produto precisa ter id inteiro, title e category com texto e price >= 0.
    Se algum estiver errado, falha com AirflowFailException (sem retry).
    Se estiver tudo certo, devolve a própria lista.
    """

    template_fields: Sequence[str] = ("produtos",)
    ui_color = "#e8f1f6"

    def __init__(self, *, produtos: Any, **kwargs) -> None:
        super().__init__(**kwargs)
        self.produtos = produtos

    @staticmethod
    def _erros_do_produto(produto: Any) -> list[str]:
        if not isinstance(produto, dict):
            return ["registro não é um objeto"]
        erros = []
        if not isinstance(produto.get("id"), int) or isinstance(produto.get("id"), bool):
            erros.append("id deve ser inteiro")
        for campo in ("title", "category"):
            valor = produto.get(campo)
            if not isinstance(valor, str) or not valor.strip():
                erros.append(f"{campo} deve ser texto não vazio")
        preco = produto.get("price")
        if isinstance(preco, bool) or not isinstance(preco, (int, float)) or preco < 0:
            erros.append("price deve ser número >= 0")
        return erros

    def execute(self, context) -> list[dict]:
        if not isinstance(self.produtos, list) or not self.produtos:
            raise AirflowFailException("Lista de produtos vazia ou inválida")

        problemas = []
        for indice, produto in enumerate(self.produtos):
            erros = self._erros_do_produto(produto)
            if erros:
                identificador = produto.get("id") if isinstance(produto, dict) else indice
                problemas.append(f"produto {identificador}: {'; '.join(erros)}")

        if problemas:
            for problema in problemas:
                log.error("Schema inválido -> %s", problema)
            raise AirflowFailException(
                f"{len(problemas)} produto(s) com schema inválido. Primeiro: {problemas[0]}"
            )

        log.info("%d produtos com schema válido", len(self.produtos))
        return self.produtos
