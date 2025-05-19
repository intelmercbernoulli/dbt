SELECT [CÓDIGO DO CLIENTE] AS codrm,
		[NOME DO CLIENTE],
		[REDE DE ENSINO],
		SEGMENTO,
    [GRUPO COLEÇÃO],
    [CATEGORIA PADRONIZADA],
	SUM([TOTAL HISTÓRICO]) AS alunos
  FROM {{ ref('slv_cubo33_filtrado_2025') }}
WHERE
    CATEGORIA IN ('Coleção', 'Coleção - 1º Semestre', 'Coleção - 2º Semestre')
    AND VOLUME IN ('1', 'Único', 'U')
group by [CÓDIGO DO CLIENTE],[NOME DO CLIENTE], [REDE DE ENSINO], SEGMENTO, [GRUPO COLEÇÃO], [CATEGORIA PADRONIZADA]
;