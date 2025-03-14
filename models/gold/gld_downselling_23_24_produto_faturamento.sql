WITH T2023 AS (
  SELECT 
    [CÓDIGO DO CLIENTE],
    [GRUPO COLEÇÃO],
    SUM([FATURAMENTO_LIQUIDO]) AS FATURAMENTO_2023
  FROM 
    {{ ref('gld_cubo33_2023_faturamento_produto') }}
  GROUP BY 
    [CÓDIGO DO CLIENTE], [GRUPO COLEÇÃO]
),
T2024 AS (
  SELECT 
    [CÓDIGO DO CLIENTE],
    [GRUPO COLEÇÃO],
    SUM([FATURAMENTO_LIQUIDO]) AS FATURAMENTO_2024
  FROM 
    {{ ref('gld_cubo33_2024_faturamento_produto') }}
  GROUP BY 
    [CÓDIGO DO CLIENTE], [GRUPO COLEÇÃO]
)
SELECT 
  T2023.[CÓDIGO DO CLIENTE],
  T2023.[GRUPO COLEÇÃO],
  T2023.FATURAMENTO_2023,
  ISNULL(T2024.FATURAMENTO_2024, 0) AS FATURAMENTO_2024,
  T2023.FATURAMENTO_2023 - ISNULL(T2024.FATURAMENTO_2024, 0) AS DIFERENÇA,
  CASE
    WHEN ISNULL(T2024.FATURAMENTO_2024, 0) < T2023.FATURAMENTO_2023 THEN 'Perdeu Faturamento'
    ELSE 'Não Perdeu Faturamento'
  END AS STATUS_FATURAMENTO
FROM 
  T2023
LEFT JOIN 
  T2024
ON 
  T2023.[CÓDIGO DO CLIENTE] = T2024.[CÓDIGO DO CLIENTE]
  AND T2023.[GRUPO COLEÇÃO] = T2024.[GRUPO COLEÇÃO];