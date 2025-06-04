-- models/staging/stg_vendas.sql

WITH source_data AS (
    SELECT
        ID_VENDA,
        DATA_VENDA,
        ID_CLIENTE,
        ID_PRODUTO,
        QUANTIDADE,
        VALOR_UNITARIO,
        VALOR_TOTAL,
        CANAL_VENDA
    FROM
        {{ source('bronze', 'raw_venda') }}
),

renamed_recast AS (
    SELECT
        ID_VENDA AS id_venda,
        DATA_VENDA AS data_venda,
        ID_CLIENTE AS id_cliente,
        ID_PRODUTO AS id_produto,
        QUANTIDADE AS quantidade,
        VALOR_UNITARIO AS valor_unitario,
        VALOR_TOTAL AS valor_total,
        CANAL_VENDA AS canal_venda
    FROM
        source_data
)

SELECT * FROM renamed_recast