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

faturamento AS (
    SELECT
        [CODIGO_AJUSTADO],
        SUM([Receita Bruta Total]) AS [Receita Bruta Total],
        SUM([Receita Líquida Total]) AS [Receita Líquida Total]
    FROM {{ source('intel_merc', 'brz_estimativas') }}
    GROUP BY [CODIGO_AJUSTADO]
),

segmentos AS (
    SELECT *
    FROM {{ ref('slv_segmentos_dos_clientes') }}
),

empresas_unificadas AS (
    SELECT
        COALESCE([Cod RM Ajustado], [Cod RM 1 Ajustado]) AS codigo_base,
        [Quantidade Total de Alunos]
    FROM {{ ref('slv_empresas_crm') }}
)

SELECT 
    a.[CÓDIGO DO CLIENTE],
    a.[NOME DO CLIENTE],
    a.[REDE DE ENSINO],
    a.[CIDADE],
    a.[UF],
    s.[SEGMENTOS],
    a.ALUNOS AS [Alunos que Compram],
    e.[Quantidade Total de Alunos] AS [Total de Alunos],
    f.[Receita Bruta Total],
    f.[Receita Líquida Total],
    f.[Receita Bruta Total] - f.[Receita Líquida Total] AS [Benefícios Concedidos],
    CASE 
        WHEN f.[Receita Bruta Total] = 0 THEN NULL
        ELSE (f.[Receita Bruta Total] - f.[Receita Líquida Total]) / f.[Receita Bruta Total]
    END AS [% Benefícios Concedidos],
    CASE 
        WHEN a.ALUNOS = 0 THEN NULL
        ELSE f.[Receita Líquida Total] / a.ALUNOS
    END AS [Ticket Médio]
FROM alunos a
LEFT JOIN faturamento f
    ON a.[CÓDIGO DO CLIENTE] = f.[CODIGO_AJUSTADO]
LEFT JOIN segmentos s
    ON a.[CÓDIGO DO CLIENTE] = s.[CÓDIGO DO CLIENTE]
LEFT JOIN empresas_unificadas e
    ON a.[CÓDIGO DO CLIENTE] COLLATE SQL_Latin1_General_CP1_CI_AI = e.codigo_base COLLATE SQL_Latin1_General_CP1_CI_AI