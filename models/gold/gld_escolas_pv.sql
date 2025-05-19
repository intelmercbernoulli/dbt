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
    WHERE [SEGMENTO] = 'PV' AND [GRUPO COLEÇÃO] = 'COLEÇÃO PRINCIPAL'
),

-- Tenta encontrar pelo Código RM
empresas_rm AS (
    SELECT 
        RIGHT('000000' + LEFT(CAST([Código RM] AS VARCHAR), CHARINDEX('.', CAST([Código RM] AS VARCHAR) + '.') - 1), 6) AS [Código RM],
        [Código INEP],
        [Tipo de Atividade]
    FROM {{ source('intel_merc', 'brz_empresas') }}
    WHERE [Código RM] IS NOT NULL
),

-- Tenta encontrar pelo Código 1 (antigo)
empresas_rm_antigo AS (
    SELECT 
        RIGHT('000000' + LEFT(CAST([Código 1] AS VARCHAR), CHARINDEX('.', CAST([Código 1] AS VARCHAR) + '.') - 1), 6) AS [Código RM],
        [Código INEP],
        [Tipo de Atividade]
    FROM {{ source('intel_merc', 'brz_empresas') }}
    WHERE [Código 1] IS NOT NULL
),

-- Junta as duas buscas, priorizando o que encontrar primeiro
empresas_final AS (
    SELECT 
        [Código RM],
        [Código INEP],
        [Tipo de Atividade],
        ROW_NUMBER() OVER (
            PARTITION BY [Código RM] 
            ORDER BY 
                CASE 
                    WHEN [Tipo de Atividade] IS NOT NULL THEN 0 
                    ELSE 1 
                END
        ) AS prioridade
    FROM empresas_rm

    UNION ALL

    SELECT 
        [Código RM],
        [Código INEP],
        [Tipo de Atividade],
        ROW_NUMBER() OVER (
            PARTITION BY [Código RM] 
            ORDER BY 
                CASE 
                    WHEN [Tipo de Atividade] IS NOT NULL THEN 0 
                    ELSE 1 
                END
        ) AS prioridade
    FROM empresas_rm_antigo
),

-- Filtra para pegar apenas o primeiro match
empresas_priorizadas AS (
    SELECT 
        [Código RM],
        [Código INEP],
        [Tipo de Atividade]
    FROM empresas_final
    WHERE prioridade = 1
),

grupo AS (
    SELECT * FROM {{ ref('slv_grupo_colecao')}}
)

SELECT 
    a.[CÓDIGO DO CLIENTE],
    a.[NOME DO CLIENTE],
    a.[REDE DE ENSINO],
    a.[CIDADE],
    a.[UF],
    a.[SÉRIE],
    a.[ALUNOS],
    e.[Código INEP],
    e.[Tipo de Atividade],
    g.[usa_extra],
    g.[usa_simulado]
FROM cubo_alunos a
LEFT JOIN empresas_priorizadas e 
    ON a.[CÓDIGO DO CLIENTE] COLLATE SQL_Latin1_General_CP1_CI_AI = e.[Código RM] COLLATE SQL_Latin1_General_CP1_CI_AI
LEFT JOIN grupo g 
    ON a.[CÓDIGO DO CLIENTE] = g.[CÓDIGO DO CLIENTE]