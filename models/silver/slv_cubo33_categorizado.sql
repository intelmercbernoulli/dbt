SELECT 
    venda.*--,
    ,categorizacao.[SÉRIE]
    ,categorizacao.[SEGMENTO]
    ,categorizacao.[GRUPO COLEÇÃO]
    ,categorizacao.[CATEGORIA PADRONIZADA]
FROM 
    {{ ref('slv_cubo33_aluno') }} AS venda
LEFT JOIN
    {{ source('intel_merc', 'brz_categorizacao_produtos') }} AS categorizacao 
ON 
    venda.[COLEÇÃO] = categorizacao.[COLEÇÃO];