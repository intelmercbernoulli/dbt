WITH faturamento AS (
    SELECT 
        [CÓDIGO DO CLIENTE],
        [NOME DO CLIENTE],
        [REDE DE ENSINO],
        [CIDADE],
        [UF],
        [ANO DE UTILIZAÇÃO],
        SUM([FATURAMENTO SEM DESCONTO]) AS [FATURAMENTO SEM DESCONTO],
        SUM([RECEITA]) AS [RECEITA]
    FROM {{ ref('slv_cubo33_aluno') }}
    GROUP BY 
        [CÓDIGO DO CLIENTE],
        [NOME DO CLIENTE],
        [REDE DE ENSINO],
        [CIDADE],
        [UF],
        [ANO DE UTILIZAÇÃO]
),

alunos AS (
    SELECT 
        [CÓDIGO DO CLIENTE],
        [ANO DE UTILIZAÇÃO],
        SUM([QUANTIDADE PEDIDO]) AS ALUNOS
    FROM {{ ref('slv_cubo33_aluno') }}
    WHERE 
        CATEGORIA IN ('Coleção', 'Coleção - 1º Semestre', 'Coleção - 2º Semestre')
        AND VOLUME IN ('1', 'Único', 'U')
    GROUP BY 
        [CÓDIGO DO CLIENTE],
        [ANO DE UTILIZAÇÃO]
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