WITH passaporte AS (

    SELECT
        id_account,
        [Ano do Término da Programação] AS ano_passaporte,
        COUNT(*) AS qtde_passaportes
    FROM {{ ref('slv_passaporte_finalizado') }}
    GROUP BY
        id_account,
        [Ano do Término da Programação]

),

oportunidades AS (

    SELECT
        [ID da Escola]      AS id_account,
        [Ano de Fechamento] AS ano_fechamento,
        receita_bruta,
        receita_liquida,
        alunos_fechados
    FROM {{ ref('gld_oportunidades_fechadas_novos_negocios_escolas') }}

),

empresas AS (

    SELECT
        accountid        AS id_account,
        [Nome Fantasia],
        [Rede de Ensino],
        [Estado],
        [Cidade],
        [Perfil da escola],
        [Gerência Regional BSE],
        [Porte],
        [Região]
    FROM {{ ref('slv_empresas_crm') }}

)

SELECT
    p.id_account,
    p.ano_passaporte,
    p.qtde_passaportes,

    -- flag de fechamento no mesmo ano
    CASE
        WHEN o.id_account IS NOT NULL THEN 1
        ELSE 0
    END AS flag_fechado_mesmo_ano,

    -- métricas comerciais
    o.receita_bruta,
    o.receita_liquida,
    o.alunos_fechados,

    -- atributos da escola
    e.[Nome Fantasia],
    e.[Rede de Ensino],
    e.[Estado],
    e.[Cidade],
    e.[Perfil da escola],
    e.[Gerência Regional BSE],
    e.[Porte],
    e.[Região]

FROM passaporte p
LEFT JOIN oportunidades o
       ON p.id_account = o.id_account
      AND p.ano_passaporte = o.ano_fechamento
LEFT JOIN empresas e
       ON p.id_account = e.id_account