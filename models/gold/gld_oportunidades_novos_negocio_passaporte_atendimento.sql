WITH oportunidades AS (

    SELECT
        [ID da Escola]                    AS id_account,
        [Nome Fantasia],
        [Id Oportunidade],
        [Negociação Referente a],
        [Oportunidade Referente a],
        [Status da negociação],
        [Fase da Venda],
        [Data de Fechamento do Contrato],
        [Tempo de Contrato],
        [Ano de Fechamento]               AS ano_fechamento,
        [Ano de Utilização],
        [Receita Bruta],
        [Receita Consultor],
        [Quantidade de Alunos Negociados],
        [Estado],
        [Cidade],
        [Perfil da escola]
    FROM {{ ref('gld_oportunidades_pre_filtradas_novos_negocios') }}

),

-- atendimentos agregados por escola + ano
atendimentos AS (

    SELECT
        id_account,
        CAST(SUBSTRING([Data do Atendimento], 7, 4) AS INT) AS ano_atendimento,
        COUNT(*) AS qtde_atendimentos
    FROM {{ source('intel_merc', 'brz_solicitacoes_crm') }}
    WHERE [Status do Agendamento] = 'Realizado pelo CO'
    GROUP BY
        id_account,
        CAST(SUBSTRING([Data do Atendimento], 7, 4) AS INT)

),

-- passaportes agregados por escola + ano
passaporte AS (

    SELECT
        id_account,
        [Ano do Término da Programação] AS ano_passaporte,
        COUNT(*) AS qtde_passaportes
    FROM {{ ref('slv_passaporte_finalizado') }}
    GROUP BY
        id_account,
        [Ano do Término da Programação]

)

SELECT
    o.*,

    -- flags individuais
    CASE
        WHEN a.id_account IS NOT NULL THEN 1
        ELSE 0
    END AS flag_atendimento_mesmo_ano,

    CASE
        WHEN p.id_account IS NOT NULL THEN 1
        ELSE 0
    END AS flag_passaporte_mesmo_ano,

    -- flag consolidada de interação
    CASE
        WHEN a.id_account IS NOT NULL
          OR p.id_account IS NOT NULL
        THEN 1
        ELSE 0
    END AS flag_interacao_mesmo_ano,

    -- métricas opcionais (se quiser analisar intensidade)
    a.qtde_atendimentos,
    p.qtde_passaportes

FROM oportunidades o
LEFT JOIN atendimentos a
       ON o.id_account    = a.id_account
      AND o.ano_fechamento = a.ano_atendimento
LEFT JOIN passaporte p
       ON o.id_account    = p.id_account
      AND o.ano_fechamento = p.ano_passaporte