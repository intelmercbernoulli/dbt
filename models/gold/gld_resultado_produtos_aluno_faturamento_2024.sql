WITH alunos AS (
    SELECT 
        TRIM([CÓDIGO DO CLIENTE]) AS [CÓDIGO DO CLIENTE],
        [NOME DO CLIENTE],
        [REDE DE ENSINO],
        [CIDADE],
        [UF],
        TRIM(SEGMENTO) AS SEGMENTO,
        TRIM([SÉRIE]) AS [SÉRIE],
        TRIM([GRUPO COLEÇÃO]) AS [GRUPO COLEÇÃO],
        [CATEGORIA PADRONIZADA],
        SUM([QUANTIDADE PEDIDO]) AS ALUNOS
    FROM {{ ref('slv_cubo33') }}
    WHERE
        ([ANO DE UTILIZAÇÃO] = '2024'
        AND [ALUNO/PROFESSOR] = 'Aluno'
        AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
        AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
        AND VOLUME IN ('1', 'Único', 'U')
        AND NOT ANO = '2025'
        AND [GRUPO COLEÇÃO] NOT IN ('CAMBRIDGE','COLEÇÃO PRINCIPAL'))
    OR 
        ([ANO DE UTILIZAÇÃO] = '2024'
        AND [ALUNO/PROFESSOR] = 'Aluno'
        AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
        AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
        AND VOLUME IN ('1', 'Único', 'U')
        AND NOT ANO = '2025'
        AND [GRUPO COLEÇÃO] = 'CAMBRIDGE'
        AND ( [DESCRIÇÃO] LIKE '%STUDENT%'
            OR [DESCRIÇÃO] LIKE '%STUDENTS%'
            OR [DESCRIÇÃO] LIKE '%STUDENT''S%'
            OR [DESCRIÇÃO] LIKE '%SB%'))
    OR 
        ([ANO DE UTILIZAÇÃO] = '2024'
        AND [GRUPO COLEÇÃO] = 'COLEÇÃO PRINCIPAL'
        AND [ALUNO/PROFESSOR] = 'Aluno'
        AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
        AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
        AND CATEGORIA IN ('Coleção', 'Coleção - 1º Semestre', 'Coleção - 2º Semestre')
        AND VOLUME IN ('1', 'Único', 'U')
        AND NOT ANO = '2025')
    GROUP BY [CÓDIGO DO CLIENTE], [NOME DO CLIENTE], [REDE DE ENSINO], [CIDADE], [UF], SEGMENTO, [SÉRIE], [GRUPO COLEÇÃO], [CATEGORIA PADRONIZADA]
),

faturamento AS (
    SELECT 
        TRIM([CÓDIGO DO CLIENTE]) AS [CÓDIGO DO CLIENTE],
        TRIM(SEGMENTO) AS SEGMENTO,
        TRIM([SÉRIE]) AS [SÉRIE],
        TRIM([GRUPO COLEÇÃO]) AS [GRUPO COLEÇÃO],
        SUM(CAST([FATURAMENTO SEM DESCONTO] AS NUMERIC(18, 2))) AS [FATURAMENTO_BRUTO],
        SUM(CAST([RECEITA] AS NUMERIC(18, 2))) AS [FATURAMENTO_LIQUIDO],
        SUM(CAST([VALOR TOTAL DESCONTO] AS NUMERIC(18, 2))) AS [VALOR_TOTAL_DESCONTO]
    FROM {{ ref('slv_cubo33') }}
    WHERE
        [ANO DE UTILIZAÇÃO] = '2024'
        AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
        AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
        AND NOT ANO = '2025'
    GROUP BY [CÓDIGO DO CLIENTE], SEGMENTO, [SÉRIE], [GRUPO COLEÇÃO]
)

SELECT 
    COALESCE(a.[CÓDIGO DO CLIENTE], f.[CÓDIGO DO CLIENTE]) AS [CÓDIGO DO CLIENTE],
    COALESCE(a.SEGMENTO, f.SEGMENTO) AS SEGMENTO,
    COALESCE(a.[SÉRIE], f.[SÉRIE]) AS [SÉRIE],
    COALESCE(a.[GRUPO COLEÇÃO], f.[GRUPO COLEÇÃO]) AS [GRUPO COLEÇÃO],
    a.[NOME DO CLIENTE],
    a.[REDE DE ENSINO],
    a.[CIDADE],
    a.[UF],
    a.[CATEGORIA PADRONIZADA],
    COALESCE(a.ALUNOS, 0) AS ALUNOS,
    COALESCE(f.[FATURAMENTO_BRUTO], 0) AS [FATURAMENTO_BRUTO],
    COALESCE(f.[FATURAMENTO_LIQUIDO], 0) AS [FATURAMENTO_LIQUIDO],
    COALESCE(f.[VALOR_TOTAL_DESCONTO], 0) AS [VALOR_TOTAL_DESCONTO]
FROM alunos a
FULL OUTER JOIN faturamento f
ON TRIM(a.[CÓDIGO DO CLIENTE]) = TRIM(f.[CÓDIGO DO CLIENTE])
AND TRIM(a.SEGMENTO) = TRIM(f.SEGMENTO)
AND TRIM(a.[SÉRIE]) = TRIM(f.[SÉRIE])
AND TRIM(a.[GRUPO COLEÇÃO]) = TRIM(f.[GRUPO COLEÇÃO]);