WITH empresas AS (
    SELECT *
    FROM {{ source('intel_merc', 'brz_empresas') }}
),

-- Considera apenas escolas que de fato podem blindar outras
origens_com_blindagem AS (
    SELECT *
    FROM empresas
    WHERE [Forma de Blindagem] IN ('Cidade', 'Bairro', 'Concorrente específico')
),

blindagens AS (
    SELECT
        alvo.accountid AS escola_alvo_id,
        origem.accountid AS escola_que_blindou_id,
        CASE
            WHEN origem.[Forma de Blindagem] = 'Cidade'
                 AND origem.Cidade = alvo.Cidade
                 AND origem.accountid <> alvo.accountid
            THEN 'Cidade'

            WHEN origem.[Forma de Blindagem] = 'Bairro'
                 AND origem.Cidade = alvo.Cidade
                 AND origem.Bairro = alvo.Bairro
                 AND origem.accountid <> alvo.accountid
            THEN 'Bairro'

            WHEN origem.[Forma de Blindagem] = 'Concorrente específico'
                 AND (
                      origem.[Concorrente específico 1] = alvo.accountid OR
                      origem.[Concorrente específico 2] = alvo.accountid OR
                      origem.[Concorrente específico 3] = alvo.accountid OR
                      origem.[Concorrente específico 4] = alvo.accountid OR
                      origem.[Concorrente específico 5] = alvo.accountid
                 )
            THEN 'Concorrente específico'

            ELSE NULL
        END AS tipo_blindagem
    FROM empresas AS alvo
    JOIN origens_com_blindagem AS origem
        ON origem.accountid <> alvo.accountid
),
primeira_blindagem AS (
    SELECT
        escola_alvo_id,
        tipo_blindagem,
        escola_que_blindou_id,
        ROW_NUMBER() OVER (PARTITION BY escola_alvo_id ORDER BY tipo_blindagem) AS ordem
    FROM blindagens
    WHERE tipo_blindagem IS NOT NULL
)

SELECT
    e.*,
    pb.tipo_blindagem AS tipo_blindagem_recebida,
    pb.escola_que_blindou_id AS escola_que_blindou,
    esf.[Nome Fantasia] AS escola_que_blindou_nome
FROM empresas e
LEFT JOIN primeira_blindagem pb
    ON e.accountid = pb.escola_alvo_id
    AND pb.ordem = 1
LEFT JOIN empresas esf
    ON pb.escola_que_blindou_id = esf.accountid
WHERE pb.tipo_blindagem IS NOT NULL