SELECT [CÓDIGO DO CLIENTE] AS [CÓDIGO DO CLIENTE],
		[NOME DO CLIENTE],
		[REDE DE ENSINO],
    [CIDADE],
    [UF],
		SEGMENTO,
	SUM([QUANTIDADE PEDIDO]) AS ALUNOS
  FROM {{ ref('slv_cubo33') }}
  WHERE
    [ANO DE UTILIZAÇÃO] = '2024'
	AND [ALUNO/PROFESSOR] = 'Aluno'
    AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
    AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
    AND CATEGORIA IN ('Coleção', 'Coleção - 1º Semestre', 'Coleção - 2º Semestre')
    AND VOLUME IN (1,'1', 'Único', 'U')
	AND NOT ANO = '2025'
    /*AND [CÓDIGO DO CLIENTE] NOT IN (
      SELECT DISTINCT ([RM_FK])
      FROM "DB_DLK_INTMERCDB"."dbo"."Premio_Expurgos_Dim"
      WHERE TIPO = 'INTERCOMPANY')*/
    --AND CODTMV <> '2.2.40' -- retirar ecommerce
  GROUP BY [CÓDIGO DO CLIENTE],[NOME DO CLIENTE], [REDE DE ENSINO], [CIDADE], [UF], SEGMENTO
;