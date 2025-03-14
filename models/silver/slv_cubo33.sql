SELECT 
    venda.*--,
    --categorizacao.[COLEÇÃO] AS COLEÇÃO_CATEGORIZACAO
    ,categorizacao.[SÉRIE]
    ,categorizacao.[SEGMENTO]
    ,categorizacao.[GRUPO COLEÇÃO]
    ,categorizacao.[CATEGORIA PADRONIZADA]
FROM 
    {{ source('intel_merc', 'brz_bivendacolecao') }} AS venda
LEFT JOIN
    {{ source('intel_merc', 'brz_categorizacao_produtos') }} AS categorizacao 
ON 
    venda.[COLEÇÃO] = categorizacao.[COLEÇÃO];