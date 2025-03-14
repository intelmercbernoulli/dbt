SELECT [CÓDIGO DO CLIENTE] AS codrm
	,[NOME DO CLIENTE]
    ,[CIDADE]
    ,[UF]
	,[ANO DE UTILIZAÇÃO]
	,CATEGORIA
	,SUM([QUANTIDADE PEDIDO]) AS alunos
  FROM {{ ref('slv_cubo33') }}
  where 
	VOLUME in (1 ,'U', 'Único')
    --AND [ANO DE UTILIZAÇÃO] = '2024' 
    AND [ALUNO/PROFESSOR] = 'Aluno' 
    AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Remessa de simulado', 'Tiragem')
	--AND CATEGORIA = 'Coleção'
    AND [CÓDIGO DO CLIENTE] NOT IN (
	SELECT DISTINCT ([RM_FK])
	FROM {{ source('intel_merc', 'Premio_Expurgos_Dim') }}
	WHERE TIPO = 'INTERCOMPANY')
	AND CODTMV <> '2.2.40' -- retirar ecommerce
  group by [CÓDIGO DO CLIENTE],[NOME DO CLIENTE]
    ,[CIDADE]
    ,[UF]
	,[ANO DE UTILIZAÇÃO], CATEGORIA;