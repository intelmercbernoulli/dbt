WITH atendimentos AS (

    SELECT
        id_account,
        Cliente,
        CAST(SUBSTRING([Data do Atendimento], 7, 4) AS INT) AS ano_atendimento,
        COUNT(*) AS qtde_atendimentos
    FROM {{ source('intel_merc', 'brz_solicitacoes_crm') }}
    WHERE [Status do Agendamento] = 'Realizado pelo CO'
    GROUP BY
        id_account,
        Cliente,
        CAST(SUBSTRING([Data do Atendimento], 7, 4) AS INT)

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
        [Liderança Regional],
        [Porte],
        [Região]
    FROM {{ ref('slv_empresas_crm') }}

)

SELECT
    a.id_account,
    a.Cliente,
    a.ano_atendimento,
    a.qtde_atendimentos,

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
    e.[Liderança Regional],
    e.[Porte],
    e.[Região]

FROM atendimentos a
LEFT JOIN oportunidades o
       ON a.id_account = o.id_account
      AND a.ano_atendimento = o.ano_fechamento
LEFT JOIN empresas e
       ON a.id_account = e.id_account