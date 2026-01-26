WITH
oportunidades AS (
    SELECT *
    FROM {{ ref('gld_oportunidades_pre_filtradas_novos_negocios') }}
),

/* Deduplicação: 1 linha por escola / ano */
passaporte_por_ano AS (
    SELECT DISTINCT
        [id_account],
        [Ano do Término da Programação] AS [Ano]
    FROM {{ ref('slv_passaporte_finalizado') }}
),

/* Deduplicação: 1 linha por escola / ano */
atendimento_por_ano AS (
    SELECT DISTINCT
        [id_account],
        [Ano de Atendimento] AS [Ano]
    FROM {{ ref('slv_atendimento_novos_negocios') }}
)

SELECT
    o.[ID da Escola],
    o.[Id Oportunidade],
    o.[Ano de Fechamento],
    o.[Receita Bruta],
    o.[Receita Consultor],
    o.[Quantidade de Alunos Negociados],
    o.[Estado],
    o.[Cidade],
    o.[Perfil da escola],

    CASE 
        WHEN p.[id_account] IS NOT NULL THEN 1
        ELSE 0
    END AS [Teve Passaporte no Ano],

    CASE 
        WHEN a.[id_account] IS NOT NULL THEN 1
        ELSE 0
    END AS [Teve Atendimento no Ano],

    CASE
        WHEN p.[id_account] IS NOT NULL
          OR a.[id_account] IS NOT NULL THEN 1
        ELSE 0
    END AS [Teve Atendimento ou Passaporte no Ano]

FROM oportunidades o

LEFT JOIN passaporte_por_ano p
    ON o.[ID da Escola] = p.[id_account]
   AND o.[Ano de Fechamento] = p.[Ano]

LEFT JOIN atendimento_por_ano a
    ON o.[ID da Escola] = a.[id_account]
   AND o.[Ano de Fechamento] = a.[Ano];