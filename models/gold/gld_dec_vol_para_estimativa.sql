WITH PivotTable AS (
    SELECT *
    FROM (
        SELECT 
            [COLEÇÃO],
            [CATEGORIA PADRONIZADA], 
            CAST([VOLUME] AS NVARCHAR) AS [VOLUME], -- Garante compatibilidade com 'Único'
            [QUANTIDADE PEDIDO]
        FROM  {{ ref('slv_cubo33_filtrado_2024') }} 
    ) AS SourceTable
    PIVOT (
        SUM([QUANTIDADE PEDIDO])
        FOR [VOLUME] IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [Único])
    ) AS PVT
)
SELECT 
    [COLEÇÃO],
    [CATEGORIA PADRONIZADA],
    [1], [2], [3], [4], [5], [6], [7], [8], [9], [Único],

    -- Cálculo da queda/aumento percentual entre volumes consecutivos
    CASE WHEN [1] > 0 THEN ([2] - [1]) / [1] ELSE NULL END AS [QUEDA VOL1/2],
    CASE WHEN [2] > 0 THEN ([3] - [2]) / [2] ELSE NULL END AS [QUEDA VOL2/3],
    CASE WHEN [3] > 0 THEN ([4] - [3]) / [3] ELSE NULL END AS [QUEDA VOL3/4],
    CASE WHEN [4] > 0 THEN ([5] - [4]) / [4] ELSE NULL END AS [QUEDA VOL4/5],
    CASE WHEN [5] > 0 THEN ([6] - [5]) / [5] ELSE NULL END AS [QUEDA VOL5/6],
    CASE WHEN [6] > 0 THEN ([7] - [6]) / [6] ELSE NULL END AS [QUEDA VOL6/7],
    CASE WHEN [7] > 0 THEN ([8] - [7]) / [7] ELSE NULL END AS [QUEDA VOL7/8],
    CASE WHEN [8] > 0 THEN ([9] - [8]) / [8] ELSE NULL END AS [QUEDA VOL8/9]
FROM PivotTable;
