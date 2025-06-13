-- models/staging/oracle_source/stg_products.sql
select
    ID_PRODUTO                          as product_id,
    NOME_PRODUTO                        as product_name,
    PRECO                               as price,
    DATA_INSERCAO                       as insertion_date,
    -- GARANTE QUE A DATA DE MODIFICAÇÃO NUNCA SERÁ NULA
    coalesce(DATA_ALTERACAO, DATA_INSERCAO) as last_modified_date
from {{ source('oracle_source', 'PRODUCTS') }}