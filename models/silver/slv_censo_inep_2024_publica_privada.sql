SELECT 
    [NO_MUNICIPIO],
    [CO_MUNICIPIO],
    [NO_REGIAO],
    [NO_UF],
    [SG_UF],
    CASE 
        WHEN [TP_DEPENDENCIA] = '4' THEN 'Particular'
        ELSE 'Pública'
    END AS tipo,
    COUNT(DISTINCT [CO_ENTIDADE]) AS Escolas,
    SUM([QT_MAT_BAS]) AS [QT_MAT_BAS],
    SUM([QT_MAT_INF]) AS [QT_MAT_INF],
    SUM([QT_MAT_FUND_AI]) AS [QT_MAT_FUND_AI],
    SUM([QT_MAT_FUND_AF]) AS [QT_MAT_FUND_AF],
    SUM([QT_MAT_MED]) AS [QT_MAT_MED]
FROM 
    {{ source('intel_merc','brz_censo_inep_2024') }}
WHERE 
    [TP_SITUACAO_FUNCIONAMENTO] = '1' 
    AND [IN_REGULAR] = '1'
GROUP BY 
    [NO_MUNICIPIO],
    [CO_MUNICIPIO],
    [NO_REGIAO],
    [NO_UF],
    [SG_UF],
    CASE 
        WHEN [TP_DEPENDENCIA] = '4' THEN 'Particular'
        ELSE 'Pública'
    END