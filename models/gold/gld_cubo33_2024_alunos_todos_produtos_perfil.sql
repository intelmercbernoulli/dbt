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
    FROM {{ ref('gld_cubo33_2024_alunos_todos_produtos') }}
),

empresas AS (
    SELECT DISTINCT  
        RIGHT('000000' + LEFT(CAST([Código RM] AS VARCHAR), CHARINDEX('.', CAST([Código RM] AS VARCHAR) + '.') - 1), 6) AS [Código RM],
        CAST(CAST([Código INEP] AS BIGINT) AS VARCHAR) AS [Código INEP]
    FROM {{ source('intel_merc', 'slv_empresas') }}
),

escolas_segmentadas AS (
    SELECT DISTINCT  
        CAST([CO_ENTIDADE] AS VARCHAR) AS [CO_ENTIDADE],
        [sugestão]
    FROM {{ source('intel_merc', 'slv_escolas_segmentadas') }}
)

SELECT 
    a.[CÓDIGO DO CLIENTE],
    a.[NOME DO CLIENTE],
    a.[REDE DE ENSINO],
    a.[CIDADE],
    a.[UF],
    a.[SEGMENTO],
    a.[SÉRIE],
    a.[GRUPO COLEÇÃO],
    a.[CATEGORIA PADRONIZADA],
    a.[ALUNOS],
    e.[Código RM],
    e.[Código INEP],
    s.[CO_ENTIDADE],
    s.[sugestão]
FROM (
    SELECT DISTINCT [CÓDIGO DO CLIENTE], [NOME DO CLIENTE], [REDE DE ENSINO], [CIDADE], [UF], [SEGMENTO], [SÉRIE], [GRUPO COLEÇÃO], [CATEGORIA PADRONIZADA], [ALUNOS]
    FROM cubo_alunos
) a
LEFT JOIN empresas e 
    ON a.[CÓDIGO DO CLIENTE] COLLATE SQL_Latin1_General_CP1_CI_AI = e.[Código RM] COLLATE SQL_Latin1_General_CP1_CI_AI
LEFT JOIN escolas_segmentadas s 
    ON e.[Código INEP] COLLATE SQL_Latin1_General_CP1_CI_AI = s.[CO_ENTIDADE] COLLATE SQL_Latin1_General_CP1_CI_AI;
