-- models/staging/stg_vendas.sql
with
    source_data as (
        -- Esta parte busca os dados da sua tabela raw_venda.
        -- A função {{ source() }} diz ao dbt para buscar os dados da fonte
        -- que você definiu no seu arquivo sources.yml.
        -- 'public' é o nome do GRUPO DE FONTES (o 'name:' do source no sources.yml).
        -- 'raw_venda' é o nome da TABELA dentro desse grupo.
        select
            id_venda,
            data_venda,
            id_cliente,
            id_produto,
            quantidade,
            valor_unitario,
            valor_total,
            canal_venda
        from {{ source("public", "raw_venda") }}
    ),

    renamed_recast as (
        -- Nesta etapa, nós renomeamos as colunas para um padrão mais consistente
        -- (letras minúsculas com underscore_entre_palavras, conhecido como snake_case).
        -- Também é um bom lugar para fazer conversões de tipo (CAST) ou pequenas
        -- limpezas (TRIM).
        select
            id_venda as id_venda,
            cast(data_venda as date) as data_venda,
            id_cliente as id_cliente,
            id_produto as id_produto,
            cast(quantidade as integer) as quantidade,
            cast(valor_unitario as numeric(10, 2)) as valor_unitario,
            cast(valor_total as numeric(10, 2)) as valor_total,
            trim(canal_venda) as canal_venda
        from source_data
    )

-- Finalmente, selecionamos todas as colunas da nossa etapa de renomeação e tratamento.
select *
from renamed_recast
