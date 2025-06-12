-- models/marts/teste.sql

select count(1) qtd
FROM {{ source('oracle_dev', 'web_produto') }} p
    INNER JOIN {{ source('oracle_dev', 'web_pessoa') }} lab