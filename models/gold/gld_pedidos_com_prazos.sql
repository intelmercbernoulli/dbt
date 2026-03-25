WITH pedidos AS (
    SELECT *
    FROM {{ ref('gld_pedidos') }}
),

grupo_faturamento AS (
    SELECT
        [Cód. Grupo]            AS CodGrupo,
        [Nome do Grupo]         AS NomeGrupo,
        [Disponibilização N/NN] AS Disponibilizacao_NN,
        [Disponibilização Resto do Brasil] AS Disponibilizacao_RB
    FROM {{ source('intel_merc', 'brz_grupo_faturamento_eskolare') }}
),

geografia AS (
    SELECT
        UF,
        Cidade,
        Regiao,
        IsCapital,
        Localidade,
        DiasParaEntrega
    FROM {{ ref('slv_dim_geografica') }}
),

enriched AS (
    SELECT
        p.*,
        -- Dimensão Grupo
        g.NomeGrupo,
        g.Disponibilizacao_NN,
        g.Disponibilizacao_RB,
        -- Dimensão Geográfica
        geo.Regiao,
        geo.IsCapital,
        geo.Localidade,
        geo.DiasParaEntrega,
        -- Disponibilização final
        CASE 
            WHEN geo.Regiao IN ('Norte','Nordeste') THEN g.Disponibilizacao_NN
            ELSE g.Disponibilizacao_RB
        END AS DisponibilizacaoFinal,
        -- Prazo de expedição
        CAST(
            CASE 
                WHEN geo.Regiao IN ('Norte','Nordeste') 
                    THEN g.Disponibilizacao_NN
                ELSE g.Disponibilizacao_RB
            END 
        AS DATE) AS prazo_expedicao
    FROM pedidos p

    LEFT JOIN grupo_faturamento g
        ON p.GrupoFaturamento = g.CodGrupo

    LEFT JOIN geografia geo
        ON p.Cidade = geo.Cidade
       AND p.UF = geo.UF

),

calculos AS (
    SELECT
        *,
        -- Prazo final (SLA completo)
        DATEADD(DAY, DiasParaEntrega, prazo_expedicao) AS prazo_final,
        -- Prazo final considerando status atual (com override CE 01)
        CASE 
            -- Override prioritário
            WHEN GrupoFaturamento = 'CE 01'
                THEN CAST(prazo_ce01 AS DATE)

            -- Já em transporte / entregue
            WHEN Status_Entrega IN (
                'Despachado',
                'Em trânsito',
                'Entrega realizada',
                'Entrega - Bernoulli',
                'Entrega - com reposição'
            )
            THEN DATEADD(DAY, DiasParaEntrega, prazo_expedicao)

            -- Em processo logístico
            WHEN Status_Entrega IN (
                'Operador logistico - Em validação',
                'Operador logistico - Em fila para separação',
                'Operador logistico - Em picking/packing',
                'Aguardando cotação de frete',
                'Aguardando aprovação do frete',
                'Aguardando gerar NF Remessa',
                'Pronto para expedir'
            )
            THEN prazo_expedicao

            -- Muito inicial
            WHEN Status_Entrega = 'Aguardando importação'
            THEN DATEADD(DAY, -8, prazo_expedicao)

            -- Inicial intermediário
            WHEN Status_Entrega IN (
                'Aguardando definição de lote',
                'Aguardando gerar NF',
                'Pedido Finalizado',
                'Aguardando liberação para despacho'
            )
            THEN DATEADD(DAY, -10, prazo_expedicao)

            ELSE NULL
        END AS prazo_final_status_atual

    FROM enriched
),

final AS (
    SELECT
        *,
        -- Status do prazo (corrigido)
        CASE 
            WHEN prazo_final_status_atual < CAST(r_DataStatusAtual AS DATE)
                THEN 'Fora do Prazo'
            ELSE 'Dentro do Prazo'
        END AS status_prazo_final
    FROM calculos
)

SELECT * FROM final