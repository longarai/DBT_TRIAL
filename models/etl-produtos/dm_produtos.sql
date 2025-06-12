-- models/etl-produtos/dm_produtos.sql
WITH web_produtocodbarras AS (
    SELECT
        produto_id,
        codbarras
    FROM {{ source('oracle', 'web_produtocodbarras') }}
    -- Lógica para pegar o código de barras principal ou o mais recente
    QUALIFY ROW_NUMBER() OVER (PARTITION BY produto_id ORDER BY st_principal DESC, id DESC) = 1
)

SELECT
    p.id AS produtoid,
    p.nome AS produto_nome,
    p.nome_apresentacao,
    bc.codbarras,
    p.vlrcustoloja,
    p.vlrcustocompra,
    p.inclusao_data,
    p.alteracao_data, -- Campo chave para o controle de mudanças
    p.nome_reduzido,
    p.st_ativo,
    p.st_vendaecommerce,
    p.st_generico,
    p.ecommerce_volume,
    p.nome_ecommerce,
    p.nome_comprador,
    p.st_descontinuado,
    tv2.tabelanome_id AS tp_cx_sep_interno_id,
    tv2.codigo AS tp_cx_sep_interno_codigo,
    tv2.vlrtexto AS tp_cx_sep_interno_texto,
    tv3.tabelanome_id AS origem_mercadoria_id,
    tv3.codigo AS origem_mercadoria_codigo,
    tv3.vlrtexto AS origem_mercadoria_texto,
    ff.nome AS nomefamiliafiscal,
    lab.nome AS nomelaboratorio,
    lab.razao_social,
    lab.cpf_cnpj,
    ln.nome AS nomelinha,
    ln.id AS linhaid,
    sg.id AS subgrupocom_id,
    sg.nome AS subgrupocom_nome,
    sgf.id AS subgrupofis_id,
    sgf.nome AS subgrupofis_nome,
    pg.id AS grupo_id,
    pg.nome AS grupo_nome,
    pg.tipo_grupo,
    um.id AS unidademedidaid,
    um.nome AS nomeunidademedida,
    um.sigla,
    ncm.id AS ncmid,
    ncm.descricao AS ncmdescricao,
    ncm.chave AS ncm_chave,
    ct.id AS classeterapeuticaid,
    ct.classeterapeutica,
    pbm.id AS pbmcartao_id,
    pbm.nome AS pbmcartao_nome,
    pce.id AS prodcategoria_ecom_id,
    pce.nome AS prodcategoria_ecom_nome,
    sp.cdtipoped AS sgc_cdtipoped_id,
    ped.descricao AS sgc_cdtipoped_descricao,
    ped.flgdestino AS sgc_cdtipoped_flgdestino,
    ped.flgtipo AS sgc_cdtipoped_flgtipo,
    sp.cdtipopedido AS sgc_cdtipopedido,
    sp.flgbloqordemcompra AS sgc_flgbloqordemcompra,
    sp.flgbloqentnf AS sgc_flgbloqentnf,
    sp.flgsepcxpedint AS sgc_flgsepcxpedint,
    sg.produtoagrupamento_id AS produtoagrupamento_id,
    pa.nome AS produtoagrupamento_nome,
    p.st_ocultahistoricovenda AS flag_considera_historico,
    p.tipo_listacontrolado AS lista_controlado_id,
    tv4.vlrtexto AS lista_controlado_descricao,
    'prod' ambiente
-- select count(1) qtd
FROM {{ source('oracle', 'web_produto') }} p
    INNER JOIN {{ source('oracle', 'web_pessoa') }} lab ON p.laboratorio_id = lab.id
    INNER JOIN {{ source('oracle', 'web_prodsubgrupo') }} sg ON p.prodsubgrupocom_id = sg.id
    INNER JOIN {{ source('oracle', 'web_prodgrupo') }} pg ON sg.prodgrupo_id = pg.id
    LEFT JOIN {{ source('oracle', 'produtos') }} sp ON p.id = sp.cdproduto
    LEFT JOIN {{ source('oracle', 'wtipopedido') }} ped ON sp.cdtipoped = ped.cdtipoped
    LEFT JOIN {{ source('oracle', 'web_familiaprodutofiscal') }} ff ON p.familiaprodutofiscal_id = ff.id
    LEFT JOIN {{ source('oracle', 'web_prodlinha') }} ln ON p.linha_id = ln.id
    LEFT JOIN {{ source('oracle', 'web_produtoagrupamento') }} pa ON sg.produtoagrupamento_id = pa.id
    LEFT JOIN {{ source('oracle', 'web_prodsubgrupo') }} sgf ON p.prodsubgrupofis_id = sgf.id
    LEFT JOIN {{ source('oracle', 'web_unidademedida') }} um ON p.comercial_unidademedia_id = um.id
    LEFT JOIN {{ source('oracle', 'web_ncm') }} ncm ON p.ncm_id = ncm.id
    LEFT JOIN {{ source('oracle', 'web_classeterapeutica') }} ct ON p.classeterapeutica_id = ct.id
    LEFT JOIN {{ source('oracle', 'web_pbmcartao') }} pbm ON p.pbmcartao_id = pbm.id
    LEFT JOIN {{ source('oracle', 'web_tabelavalor') }} tv2 ON p.tp_cx_sep_interno = tv2.id
    LEFT JOIN {{ source('oracle', 'web_tabelavalor') }} tv3 ON p.origem_mercadoria = tv3.id
    LEFT JOIN {{ source('oracle', 'web_prodcategoria_ecom') }} pce ON p.prodcatgoria_ecom_id = pce.id
    LEFT JOIN {{ source('oracle', 'web_tabelavalor') }} tv4 ON p.tipo_listacontrolado = tv4.id
    LEFT JOIN web_produtocodbarras bc ON p.id = bc.produto_id

{% if is_incremental() %}

  -- Filtra para buscar apenas registros que foram alterados
  -- desde a última execução bem-sucedida do dbt
   where p.alteracao_data > (select max(alteracao_data) from {{ this }})

{% endif %}