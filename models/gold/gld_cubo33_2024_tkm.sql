WITH cte_valores AS (
    SELECT 
        [CÓDIGO DO CLIENTE] AS codrm,
        SEGMENTO,
        [GRUPO COLEÇÃO],
        [CATEGORIA PADRONIZADA],
        ROUND(SUM(CAST([VALOR TABELA] as float)), 2) AS valor_tabela, 
        ROUND(SUM(CAST([FATURAMENTO SEM DESCONTO] as float)), 2) AS fat_sem_desc, 
        ROUND(SUM(CAST([RECEITA]as float)), 2) AS receita, 
        ROUND(SUM(CAST([VALOR TOTAL DESCONTO] AS float)), 2) AS total_des
    FROM 
        {{ ref('slv_cubo33') }}
    WHERE 
        [ANO DE UTILIZAÇÃO] = '2024' 
        AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
        AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
        AND NOT ANO = '2025'
    GROUP BY [CÓDIGO DO CLIENTE], SEGMENTO, [GRUPO COLEÇÃO], [CATEGORIA PADRONIZADA]
),
cte_alunos AS (
    SELECT 
        [CÓDIGO DO CLIENTE] AS codrm,
        SEGMENTO,
        [GRUPO COLEÇÃO],
        [CATEGORIA PADRONIZADA],
        SUM([QUANTIDADE PEDIDO]) AS alunos
    FROM 
        {{ ref('slv_cubo33') }}
    WHERE
        [ANO DE UTILIZAÇÃO] = '2024' 
        AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
        AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
        AND CATEGORIA IN ('Coleção', 'Coleção - 1º Semestre', 'Coleção - 2º Semestre')
        AND VOLUME IN ('1', 'Único')
        AND NOT ANO = '2025'
    GROUP BY [CÓDIGO DO CLIENTE], SEGMENTO, [GRUPO COLEÇÃO], [CATEGORIA PADRONIZADA]
)
SELECT 
    v.codrm,
    v.SEGMENTO,
    v.[GRUPO COLEÇÃO],
    v.[CATEGORIA PADRONIZADA],
    v.valor_tabela,
    v.fat_sem_desc,
    v.receita,
    v.total_des,
    a.alunos
FROM cte_valores v
LEFT JOIN cte_alunos a
    ON v.codrm = a.codrm
    AND v.SEGMENTO = a.SEGMENTO
    AND v.[GRUPO COLEÇÃO] = a.[GRUPO COLEÇÃO]
    AND v.[CATEGORIA PADRONIZADA] = a.[CATEGORIA PADRONIZADA]
