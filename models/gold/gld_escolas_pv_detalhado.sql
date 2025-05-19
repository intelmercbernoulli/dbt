WITH cubo_alunos AS (
    SELECT DISTINCT  
        RIGHT('000000' + LEFT([CÓDIGO DO CLIENTE], CHARINDEX('.', [CÓDIGO DO CLIENTE] + '.') - 1), 6) AS [CÓDIGO DO CLIENTE],
        [NOME DO CLIENTE],
        [REDE DE ENSINO],
        [CIDADE],
        [UF],
        [SEGMENTO],
        [SÉRIE],
        [GRUPO COLEÇÃO],
        [CATEGORIA PADRONIZADA],
        [ALUNOS]
    FROM {{ ref('gld_2025_alunos_todos_produtos') }}
    WHERE [CÓDIGO DO CLIENTE] IN (
        SELECT DISTINCT([CÓDIGO DO CLIENTE]) FROM {{ ref('gld_2025_alunos_todos_produtos') }}
        WHERE [SEGMENTO] = 'PV')
),

empresas AS (
    SELECT DISTINCT  
        RIGHT('000000' + LEFT(CAST([Código RM] AS VARCHAR), CHARINDEX('.', CAST([Código RM] AS VARCHAR) + '.') - 1), 6) AS [Código RM],
        RIGHT('000000' + LEFT(CAST([Código 1] AS VARCHAR), CHARINDEX('.', CAST([Código 1] AS VARCHAR) + '.') - 1), 6) AS [Código RM antigo],
        [Código INEP],
        [Tipo de Atividade]
    FROM {{ source('intel_merc', 'brz_empresas') }}
)

SELECT 
    a.*,
    e.[Código INEP],
    e.[Tipo de Atividade]
FROM cubo_alunos a
LEFT JOIN empresas e 
    ON a.[CÓDIGO DO CLIENTE] COLLATE SQL_Latin1_General_CP1_CI_AI = e.[Código RM] COLLATE SQL_Latin1_General_CP1_CI_AI
    OR a.[CÓDIGO DO CLIENTE] COLLATE SQL_Latin1_General_CP1_CI_AI = e.[Código RM antigo] COLLATE SQL_Latin1_General_CP1_CI_AI