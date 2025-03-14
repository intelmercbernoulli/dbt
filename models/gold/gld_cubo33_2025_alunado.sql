SELECT [CÓDIGO DO CLIENTE] AS codrm,
		[NOME DO CLIENTE],
		[REDE DE ENSINO],
		SEGMENTO,
    [GRUPO COLEÇÃO],
    [CATEGORIA PADRONIZADA],
	SUM([TOTAL HISTÓRICO]) AS alunos
  FROM {{ ref('slv_cubo33_filtrado_2025') }}
  WHERE
    AND CATEGORIA IN ('Coleção', 'Coleção - 1º Semestre', 'Coleção - 2º Semestre')
    AND VOLUME IN ('1', 'Único', 'U')
    /*AND [CÓDIGO DO CLIENTE] NOT IN (
      SELECT DISTINCT ([RM_FK])
      FROM "DB_DLK_INTMERCDB"."dbo"."Premio_Expurgos_Dim"
      WHERE TIPO = 'INTERCOMPANY')*/
    --AND CODTMV <> '2.2.40' -- retirar ecommerce
  group by [CÓDIGO DO CLIENTE],[NOME DO CLIENTE], [REDE DE ENSINO], SEGMENTO, [GRUPO COLEÇÃO], [CATEGORIA PADRONIZADA]
;