-- Grain: 1 linha por empresa (accountid) por ano de fechamento da oportunidade
WITH empresas AS (

    SELECT *
    FROM {{ ref('slv_empresas_crm') }}

),

oportunidades AS (

    SELECT
        [ID da Escola]        AS id_escola,
        [Ano de Fechamento]  AS ano,
        SUM([Receita Bruta])       AS receita_bruta,
        SUM([Receita Consultor])   AS receita_consultor
    FROM {{ ref('gld_oportunidades_pre_filtradas_novos_negocios') }}
    GROUP BY
        [ID da Escola],
        [Ano de Fechamento]

),

atendimentos AS (

    SELECT
        [id_account]         AS id_escola,
        [Ano de Atendimento] AS ano,
        COUNT(*)             AS qtde_atendimentos
    FROM {{ ref('slv_atendimento_novos_negocios') }}
    GROUP BY
        [id_account],
        [Ano de Atendimento]

),

passaportes AS (

    SELECT
        [id_account]                    AS id_escola,
        [Ano do Término da Programação] AS ano,
        COUNT(*)                        AS qtde_passaportes
    FROM {{ ref('slv_passaporte_finalizado') }}
    GROUP BY
        [id_account],
        [Ano do Término da Programação]

)

SELECT
    e.*,

    o.ano,
    o.receita_bruta,
    o.receita_consultor,

    a.qtde_atendimentos,
    p.qtde_passaportes,

    /* 1. Atendimento no mesmo ano */
    CASE
        WHEN a.id_escola IS NOT NULL THEN 1
        ELSE 0
    END AS atendimento_no_mesmo_ano,

    /* 2. Passaporte no mesmo ano */
    CASE
        WHEN p.id_escola IS NOT NULL THEN 1
        ELSE 0
    END AS passaporte_no_mesmo_ano,

    /* 3. Interação no mesmo ano (atendimento OU passaporte) */
    CASE
        WHEN a.id_escola IS NOT NULL
          OR p.id_escola IS NOT NULL
        THEN 1
        ELSE 0
    END AS interacao_no_mesmo_ano

FROM empresas e

LEFT JOIN oportunidades o
    ON e.accountid = o.id_escola

LEFT JOIN atendimentos a
    ON e.accountid = a.id_escola
   AND o.ano = a.ano

LEFT JOIN passaportes p
    ON e.accountid = p.id_escola
   AND o.ano = p.ano
