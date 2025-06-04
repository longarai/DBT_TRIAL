-- models/marts/fct_vendas_diarias.sql
{{
    config(
        materialized='incremental',
        unique_key=['data_venda', 'canal_venda']
    )
}}

with
    vendas_staging as (
        select 
            data_venda, 
            canal_venda, 
            quantidade, 
            valor_total
        from {{ ref("stg_vendas") }}

        {% if is_incremental() %}
        -- Em execuções incrementais, processamos apenas os registros de stg_vendas
        -- cuja data_venda é posterior à última data_venda já existente na tabela de destino (this).
        where data_venda > (select max(data_venda) from {{ this }})
        {% endif %}
    ),

    agregacao_diaria as (
        -- Sua lógica de agregação original permanece a mesma.
        -- Ela será aplicada aos dados completos na primeira execução,
        -- e apenas aos dados novos (filtrados acima) nas execuções incrementais.
        select
            data_venda,
            canal_venda,
            sum(quantidade) as quantidade_total_dia_canal,
            sum(valor_total) as valor_total_dia_canal,
            sum(
                case when canal_venda = 'ONLINE' then quantidade else 0 end
            ) as quantidade_online  -- REGRA DE NEGÓCIO FAKE PARA TESTES
        from vendas_staging
        group by data_venda, canal_venda
    )

select
    data_venda,
    canal_venda,
    quantidade_total_dia_canal,
    valor_total_dia_canal,
    quantidade_online
from agregacao_diaria
order by data_venda, canal_venda -- Opcional no modelo final, mas útil para testes