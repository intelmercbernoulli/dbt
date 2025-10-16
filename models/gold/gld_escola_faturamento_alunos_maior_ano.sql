WITH maior_ano AS (
    SELECT 
        [CÓDIGO DO CLIENTE],
        MAX([ANO DE UTILIZAÇÃO]) AS [ANO DE UTILIZAÇÃO]
    FROM {{ ref('slv_cubo33_categorizado') }}
    GROUP BY [CÓDIGO DO CLIENTE]
),

faturamento AS (
    SELECT 
        c.[CÓDIGO DO CLIENTE],
        c.[NOME DO CLIENTE],
        c.[REDE DE ENSINO],
        c.[CIDADE],
        c.[UF],
        c.[ANO DE UTILIZAÇÃO],
        SUM(c.[FATURAMENTO SEM DESCONTO]) AS [FATURAMENTO SEM DESCONTO],
        SUM(c.[RECEITA]) AS [RECEITA]
    FROM {{ ref('slv_cubo33_aluno') }} c
    INNER JOIN maior_ano m
        ON c.[CÓDIGO DO CLIENTE] = m.[CÓDIGO DO CLIENTE]
       AND c.[ANO DE UTILIZAÇÃO] = m.[ANO DE UTILIZAÇÃO]
    GROUP BY 
        c.[CÓDIGO DO CLIENTE],
        c.[NOME DO CLIENTE],
        c.[REDE DE ENSINO],
        c.[CIDADE],
        c.[UF],
        c.[ANO DE UTILIZAÇÃO]
),

alunos AS (
    SELECT 
        c.[CÓDIGO DO CLIENTE],
        c.[ANO DE UTILIZAÇÃO],
        SUM(c.[QUANTIDADE PEDIDO]) AS ALUNOS
    FROM {{ ref('slv_cubo33_aluno') }} c
    INNER JOIN maior_ano m
        ON c.[CÓDIGO DO CLIENTE] = m.[CÓDIGO DO CLIENTE]
       AND c.[ANO DE UTILIZAÇÃO] = m.[ANO DE UTILIZAÇÃO]
    WHERE 
        c.CATEGORIA IN ('Coleção', 'Coleção - 1º Semestre', 'Coleção - 2º Semestre')
        AND c.VOLUME IN ('1', 'Único', 'U')
    GROUP BY 
        c.[CÓDIGO DO CLIENTE],
        c.[ANO DE UTILIZAÇÃO]
)

SELECT 
    f.[CÓDIGO DO CLIENTE],
    f.[NOME DO CLIENTE],
    f.[REDE DE ENSINO],
    f.[CIDADE],
    f.[UF],
    f.[ANO DE UTILIZAÇÃO],
    f.[FATURAMENTO SEM DESCONTO],
    f.[RECEITA],
    a.ALUNOS
FROM faturamento f
LEFT JOIN alunos a
    ON f.[CÓDIGO DO CLIENTE] = a.[CÓDIGO DO CLIENTE]
   AND f.[ANO DE UTILIZAÇÃO] = a.[ANO DE UTILIZAÇÃO]