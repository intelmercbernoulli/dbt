SELECT 
    -- Seleciona todas as colunas da tabela `venda`, exceto `RECEITA`
    {{ dbt_utils.star(source('intel_merc', 'brz_bivendacolecao'), except=["RECEITA", "COLEÇÃO"]) }},

    -- Mantém explicitamente a COLEÇÃO da tabela venda
    venda.[COLEÇÃO],

    -- Conversão das colunas necessárias
    TRY_CAST(venda.[QUANTIDADE PEDIDO] AS INT) AS QUANTIDADE_PEDIDO,
    TRY_CAST(venda.[FATURAMENTO SEM DESCONTO] AS FLOAT) AS FATURAMENTO_SEM_DESCONTO,
    TRY_CAST(venda.[VALOR TOTAL DESCONTO] AS FLOAT) AS VALOR_TOTAL_DESCONTO,

    -- Colunas da categorização
    categorizacao.[SÉRIE],
    categorizacao.[SEGMENTO],
    categorizacao.[GRUPO COLEÇÃO],
    categorizacao.[CATEGORIA PADRONIZADA]

FROM 
    {{ source('intel_merc', 'brz_bivendacolecao') }} AS venda
LEFT JOIN
    {{ source('intel_merc', 'brz_categorizacao_produtos') }} AS categorizacao 
ON 
    venda.[COLEÇÃO] COLLATE SQL_Latin1_General_CP1_CI_AS = categorizacao.[COLEÇÃO] COLLATE SQL_Latin1_General_CP1_CI_AS;