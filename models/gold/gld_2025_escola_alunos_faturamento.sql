WITH base_filtrada AS (
    SELECT *
    FROM {{ ref('slv_cubo33_categorizado') }}
    WHERE 
        [ANO DE UTILIZAÇÃO] = '2025'
        AND [GRUPO COLEÇÃO] = 'COLEÇÃO PRINCIPAL'
        AND [ALUNO/PROFESSOR] = 'Aluno'
        AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
        AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
        AND CATEGORIA IN ('Coleção', 'Coleção - 1º Semestre', 'Coleção - 2º Semestre')
        AND VOLUME IN ('1', 'Único', 'U')
),

alunos AS (
    SELECT 
        [CÓDIGO DO CLIENTE],
        [NOME DO CLIENTE],
        [REDE DE ENSINO],
        [CIDADE],
        [UF],
        SUM([QUANTIDADE PEDIDO]) AS ALUNOS
    FROM base_filtrada
    GROUP BY [CÓDIGO DO CLIENTE], [NOME DO CLIENTE], [REDE DE ENSINO], [CIDADE], [UF]
),

faturamento_base AS (
    SELECT *
    FROM {{ ref('slv_cubo33_categorizado') }}
    WHERE 
        [ANO DE UTILIZAÇÃO] = '2025'
        AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
        AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
),

segmentos AS (
    SELECT 
        [CÓDIGO DO CLIENTE],
        STRING_AGG([SEGMENTO], ', ') AS SEGMENTOS
    FROM (
        SELECT DISTINCT [CÓDIGO DO CLIENTE], [SEGMENTO]
        FROM faturamento_base
        WHERE [SEGMENTO] NOT IN ('PLANEJAMENTO DIÁRIO')
    ) AS sub
    GROUP BY [CÓDIGO DO CLIENTE]
),

faturamento AS (
    SELECT 
        c.[CÓDIGO DO CLIENTE],
        SUM(c.[FATURAMENTO SEM DESCONTO]) AS [FATURAMENTO SEM DESCONTO],
        SUM(c.[RECEITA]) AS [RECEITA],
        SUM(c.[VALOR TOTAL DESCONTO]) AS [VALOR TOTAL DESCONTO],
        s.SEGMENTOS
    FROM faturamento_base c
    LEFT JOIN segmentos s
        ON c.[CÓDIGO DO CLIENTE] = s.[CÓDIGO DO CLIENTE]
    GROUP BY c.[CÓDIGO DO CLIENTE], s.SEGMENTOS
),

empresas_unificadas AS (
    SELECT 
        [CÓDIGO DO CLIENTE],
        MAX([Quantidade Total de Alunos]) AS [Quantidade Total de Alunos]
    FROM (
        SELECT 
            [Cod RM Ajustado] COLLATE SQL_Latin1_General_CP1_CI_AI AS [CÓDIGO DO CLIENTE],
            [Quantidade Total de Alunos]
        FROM {{ ref('slv_empresas_crm') }}
        
        UNION ALL

        SELECT 
            [Cod RM 1 Ajustado] COLLATE SQL_Latin1_General_CP1_CI_AI AS [CÓDIGO DO CLIENTE],
            [Quantidade Total de Alunos]
        FROM {{ ref('slv_empresas_crm') }}
    ) base
    GROUP BY [CÓDIGO DO CLIENTE]
)

SELECT 
    a.[CÓDIGO DO CLIENTE],
    a.[NOME DO CLIENTE],
    a.[REDE DE ENSINO],
    a.[CIDADE],
    a.[UF],
    a.ALUNOS [Alunos que Compram],
    e.[Quantidade Total de Alunos] [Total de Alunos],
    f.[FATURAMENTO SEM DESCONTO],
    f.[RECEITA],
    f.[SEGMENTOS],
    f.[FATURAMENTO SEM DESCONTO] - f.[RECEITA] AS [DESCONTO],
    CASE 
        WHEN f.[FATURAMENTO SEM DESCONTO] = 0 THEN NULL
        ELSE (f.[FATURAMENTO SEM DESCONTO] - f.[RECEITA]) / f.[FATURAMENTO SEM DESCONTO]
    END AS [% DESCONTO],
    CASE 
        WHEN a.ALUNOS = 0 THEN NULL
        ELSE f.[RECEITA] / a.ALUNOS
    END AS [Ticket Médio]
    
FROM alunos a
LEFT JOIN faturamento f
    ON a.[CÓDIGO DO CLIENTE] = f.[CÓDIGO DO CLIENTE]
LEFT JOIN empresas_unificadas e
    ON a.[CÓDIGO DO CLIENTE] COLLATE SQL_Latin1_General_CP1_CI_AI = e.[CÓDIGO DO CLIENTE]
