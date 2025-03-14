SELECT 
  [CÓDIGO DO CLIENTE] AS [CÓDIGO DO CLIENTE],
	[NOME DO CLIENTE],
	[REDE DE ENSINO],
  [CIDADE],
  [UF],
	SEGMENTO,
  [SÉRIE],
  [GRUPO COLEÇÃO],
  [CATEGORIA PADRONIZADA],
  SUM([QUANTIDADE PEDIDO]) AS ALUNOS
  FROM {{ ref('slv_cubo33') }}
  WHERE
(
    [ANO DE UTILIZAÇÃO] = '2022'
    AND [ALUNO/PROFESSOR] = 'Aluno'
    AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
    AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
    AND VOLUME IN (1, '1', 'Único', 'U')
    --AND NOT ANO = '2024'
    AND [GRUPO COLEÇÃO] IN ('EU NO MUNDO', 'DIVE.B')
)
OR (
    [ANO DE UTILIZAÇÃO] = '2022'
    AND [ALUNO/PROFESSOR] = 'Aluno'
    AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
    AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
    AND VOLUME IN (1, '1', 'Único', 'U')
    --AND NOT ANO = '2024'
    AND [GRUPO COLEÇÃO] = 'CAMBRIDGE'
    AND ( [DESCRIÇÃO] LIKE '%STUDENT%'
        OR [DESCRIÇÃO] LIKE '%STUDENTS%'
        OR [DESCRIÇÃO] LIKE '%STUDENT''S%'
        OR [DESCRIÇÃO] LIKE '%SB%'
    )
)
OR ([ANO DE UTILIZAÇÃO] = '2022'
    AND [GRUPO COLEÇÃO] = 'COLEÇÃO PRINCIPAL'
	  AND [ALUNO/PROFESSOR] = 'Aluno'
    AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
    AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
    AND CATEGORIA IN ('Coleção', 'Coleção - 1º Semestre', 'Coleção - 2º Semestre')
    AND VOLUME IN (1,'1', 'Único', 'U')
	--AND NOT ANO = '2024'
)
GROUP BY [CÓDIGO DO CLIENTE],[NOME DO CLIENTE], [REDE DE ENSINO], [CIDADE], [UF], SEGMENTO, [SÉRIE], [GRUPO COLEÇÃO], [CATEGORIA PADRONIZADA]