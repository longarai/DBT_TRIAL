-- models/marts/fct_vendas_diarias.sql
with
    vendas_staging as (
        select data_venda, canal_venda, quantidade, valor_total
        from {{ ref("stg_vendas") }}
    ),

    agregacao_diaria as (
        select
            data_venda,
            canal_venda,
            sum(quantidade) as quantidade_total_dia_canal,
            sum(valor_total) as valor_total_dia_canal,
            sum(
                case when canal_venda = 'ONLINE' then quantidade else 0 end
            ) as quantidade_online  -- REGRA DE NEGÓCIO FAKE PARA TESTES
        from vendas_staging  -- Aqui você usaria a CTE que referencia stg_vendas
        group by data_venda, canal_venda
    )

-- SELECT * FROM agregacao_diaria
select
    data_venda,
    canal_venda,
    quantidade_total_dia_canal,
    valor_total_dia_canal,
    quantidade_online
from agregacao_diaria
order by data_venda, canal_venda
