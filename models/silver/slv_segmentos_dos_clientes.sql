WITH faturamento_base AS (
    SELECT *
    FROM {{ ref('slv_cubo33_categorizado') }}
    WHERE 
        [ANO DE UTILIZAÇÃO] = '2025'
        AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
        AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
),

segmentos_distintos AS (
    SELECT DISTINCT 
        [CÓDIGO DO CLIENTE], 
        [SEGMENTO]
    FROM faturamento_base
    WHERE [SEGMENTO] <> 'PLANEJAMENTO DIÁRIO'
),

segmentos AS (
    SELECT 
        [CÓDIGO DO CLIENTE],
        STRING_AGG([SEGMENTO], ', ') AS SEGMENTOS
    FROM segmentos_distintos
    GROUP BY [CÓDIGO DO CLIENTE]
)

SELECT *
FROM segmentos