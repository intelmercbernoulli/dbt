WITH faturamento AS (

    SELECT 
        [CÓDIGO DO CLIENTE],
        [NOME DO CLIENTE],
        [REDE DE ENSINO],
        [CIDADE],
        [UF],
        SEGMENTO,
        [SÉRIE],
        CATEGORIA,
        [COLEÇÃO],
        [GRUPO COLEÇÃO],
        [CATEGORIA PADRONIZADA],
        SUM([FATURAMENTO SEM DESCONTO]) AS [FATURAMENTO SEM DESCONTO],
        SUM([RECEITA]) AS [RECEITA]
    FROM {{ ref('slv_cubo33_categorizado') }}
    WHERE 
        [ANO DE UTILIZAÇÃO] = '2025'
    GROUP BY 
        [CÓDIGO DO CLIENTE],
        [NOME DO CLIENTE],
        [REDE DE ENSINO],
        [CIDADE],
        [UF],
        SEGMENTO,
        [SÉRIE],
        CATEGORIA,
        [COLEÇÃO],
        [GRUPO COLEÇÃO],
        [CATEGORIA PADRONIZADA]

),

alunos AS (

    SELECT 
        [CÓDIGO DO CLIENTE],
        SEGMENTO,
        [SÉRIE],
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
    GROUP BY 
        [CÓDIGO DO CLIENTE],
        SEGMENTO,
        [SÉRIE],
        CATEGORIA,
        [COLEÇÃO],
        [GRUPO COLEÇÃO],
        [CATEGORIA PADRONIZADA]

)

SELECT 
    COALESCE(f.[CÓDIGO DO CLIENTE], a.[CÓDIGO DO CLIENTE]) AS [CÓDIGO DO CLIENTE],
    COALESCE(f.SEGMENTO, a.SEGMENTO) AS SEGMENTO,
    COALESCE(f.[SÉRIE], a.[SÉRIE]) AS [SÉRIE],
    COALESCE(f.CATEGORIA, a.CATEGORIA) AS CATEGORIA,
    COALESCE(f.[COLEÇÃO], a.[COLEÇÃO]) AS [COLEÇÃO],
    COALESCE(f.[GRUPO COLEÇÃO], a.[GRUPO COLEÇÃO]) AS [GRUPO COLEÇÃO],
    COALESCE(f.[CATEGORIA PADRONIZADA], a.[CATEGORIA PADRONIZADA]) AS [CATEGORIA PADRONIZADA],

    f.[FATURAMENTO SEM DESCONTO],
    f.[RECEITA],
    a.ALUNOS,

    CASE 
        WHEN f.[CÓDIGO DO CLIENTE] IS NOT NULL 
         AND a.[CÓDIGO DO CLIENTE] IS NOT NULL THEN 'MATCH'
        WHEN f.[CÓDIGO DO CLIENTE] IS NULL THEN 'SÓ_ALUNO'
        WHEN a.[CÓDIGO DO CLIENTE] IS NULL THEN 'SÓ_FATURAMENTO'
    END AS STATUS_JOIN

FROM faturamento f
FULL OUTER JOIN alunos a
    ON  f.[CÓDIGO DO CLIENTE] = a.[CÓDIGO DO CLIENTE]
    AND f.SEGMENTO = a.SEGMENTO
    AND f.[SÉRIE] = a.[SÉRIE]
    AND f.CATEGORIA = a.CATEGORIA
    AND f.[COLEÇÃO] = a.[COLEÇÃO]
    AND f.[GRUPO COLEÇÃO] = a.[GRUPO COLEÇÃO]
    AND f.[CATEGORIA PADRONIZADA] = a.[CATEGORIA PADRONIZADA]
