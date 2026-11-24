-- =============================================================================
-- ShopBrasil – base analítica (roda automaticamente na 1ª subida do postgres-dw)
-- =============================================================================

-- SNAPSHOT: uma linha por (dia, categoria). A PRIMARY KEY impede duplicidade;
-- o pipeline ainda faz DELETE + INSERT na mesma transação (idempotência).
CREATE TABLE IF NOT EXISTS metricas_categoria (
    data_referencia DATE          NOT NULL,
    categoria       VARCHAR(100)  NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    preco_medio     NUMERIC(10,2) NOT NULL,
    preco_min       NUMERIC(10,2) NOT NULL,
    preco_max       NUMERIC(10,2) NOT NULL,
    dag_run_id      VARCHAR(250),
    atualizado_em   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (data_referencia, categoria)
);

-- HISTÓRICO (opcional do enunciado): modo APPEND, uma linha por execução e
-- categoria, para acompanhar a evolução dos preços. UNIQUE(run_id, categoria)
-- evita duplicar quando a MESMA run é reprocessada (clear/retry).
CREATE TABLE IF NOT EXISTS historico_metricas_categoria (
    id            BIGSERIAL     PRIMARY KEY,
    run_id        VARCHAR(250)  NOT NULL,
    data_execucao DATE          NOT NULL,
    categoria     VARCHAR(100)  NOT NULL,
    qtd_produtos  INTEGER       NOT NULL,
    preco_medio   NUMERIC(10,2) NOT NULL,
    preco_min     NUMERIC(10,2) NOT NULL,
    preco_max     NUMERIC(10,2) NOT NULL,
    inserido_em   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, categoria)
);

CREATE INDEX IF NOT EXISTS idx_hist_categoria_data
    ON historico_metricas_categoria (categoria, data_execucao);

-- View "painel": o que a equipe de pricing abre de manhã (snapshot mais recente)
CREATE OR REPLACE VIEW v_painel_precos AS
SELECT categoria, qtd_produtos, preco_medio, preco_min, preco_max, data_referencia
FROM metricas_categoria
WHERE data_referencia = (SELECT MAX(data_referencia) FROM metricas_categoria)
ORDER BY categoria;
