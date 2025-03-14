SELECT 
    [GRUPO COLEÇÃO],
    SUM(
        CASE WHEN STATUS_FATURAMENTO = 'Perdeu Faturamento' 
                AND FATURAMENTO_2024 <> 0 
            THEN DIFERENÇA ELSE 0 END) AS TOTAL_PERDA_FATURAMENTO,
    SUM(FATURAMENTO_2023) AS TOTAL_FATURAMENTO_2023,
    CASE 
        WHEN SUM(FATURAMENTO_2023) = 0 THEN 0 -- Evita divisão por zero
        ELSE CAST(SUM(
            CASE WHEN STATUS_FATURAMENTO = 'Perdeu Faturamento' 
                AND FATURAMENTO_2024 <> 0 
            THEN DIFERENÇA ELSE 0 END) AS FLOAT) 
             / SUM(FATURAMENTO_2023) * 100
    END AS PERCENTUAL_PERDA
FROM 
    {{ ref('gld_downselling_23_24_produto_faturamento') }}
GROUP BY 
    [GRUPO COLEÇÃO];