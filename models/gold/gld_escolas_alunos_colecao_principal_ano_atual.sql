SELECT 
    [CÓDIGO DO CLIENTE],
    [NOME DO CLIENTE],
    [REDE DE ENSINO],
    [CIDADE],
    [UF],
	SEGMENTO,
    [SÉRIE],
    [GRUPO COLEÇÃO],
    [CATEGORIA PADRONIZADA],
    SUM([QUANTIDADE PEDIDO]) AS ALUNOS
FROM {{ ref('slv_cubo33_categorizado') }}
WHERE
        ([ANO DE UTILIZAÇÃO] = '2025'
        AND [GRUPO COLEÇÃO] = 'COLEÇÃO PRINCIPAL'
        AND [ALUNO/PROFESSOR] = 'Aluno'
        AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
        AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
        AND CATEGORIA IN ('Coleção', 'Coleção - 1º Semestre', 'Coleção - 2º Semestre')
        AND VOLUME IN ('1', 'Único', 'U'))
GROUP BY [CÓDIGO DO CLIENTE],[NOME DO CLIENTE], [REDE DE ENSINO], [CIDADE], [UF], SEGMENTO, [SÉRIE], [GRUPO COLEÇÃO], [CATEGORIA PADRONIZADA]