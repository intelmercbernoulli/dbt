SELECT 
    [CÓDIGO DO CLIENTE],
    [NOME DO CLIENTE],
    [REDE DE ENSINO],
    [CIDADE],
    [UF],
	SEGMENTO,
    [SÉRIE],
    [DESCRIÇÃO],
    CATEGORIA,
    [COLEÇÃO],
    [GRUPO COLEÇÃO],
    [CATEGORIA PADRONIZADA],
    SUM([QUANTIDADE PEDIDO]) AS ALUNOS
FROM {{ ref('slv_cubo33_categorizado') }}
WHERE
        ([ANO DE UTILIZAÇÃO] = '2025'
        AND VOLUME IN ('1', 'Único', 'U')
        AND [GRUPO COLEÇÃO] NOT IN ('CAMBRIDGE','COLEÇÃO PRINCIPAL'))
    OR 
        ([ANO DE UTILIZAÇÃO] = '2025'
        AND VOLUME IN ('1', 'Único', 'U')
        AND [GRUPO COLEÇÃO] = 'CAMBRIDGE'
        AND ( [DESCRIÇÃO] LIKE '%STUDENT%'
            OR [DESCRIÇÃO] LIKE '%STUDENTS%'
            OR [DESCRIÇÃO] LIKE '%STUDENT''S%'
            OR [DESCRIÇÃO] LIKE '%SB%'))
    OR 
        ([ANO DE UTILIZAÇÃO] = '2025'
        AND CATEGORIA IN ('Coleção', 'Coleção - 1º Semestre', 'Coleção - 2º Semestre')
        AND VOLUME IN ('1', 'Único', 'U'))
GROUP BY [CÓDIGO DO CLIENTE],[NOME DO CLIENTE], [REDE DE ENSINO], [CIDADE], [UF],  SEGMENTO, [SÉRIE],[DESCRIÇÃO],CATEGORIA,[COLEÇÃO], [GRUPO COLEÇÃO], [CATEGORIA PADRONIZADA]