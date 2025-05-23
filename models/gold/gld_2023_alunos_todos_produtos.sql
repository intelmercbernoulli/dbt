SELECT 
    venda.[Código do Cliente],
    venda.[Nome do Cliente],
    venda.[Rede] AS [REDE DE ENSINO],
    venda.[Cidade],
    venda.[UF],
    venda.[Segmento],
    venda.[Série],
    venda.[Coleção] AS [GRUPO COLEÇÃO],
    categorizacao.[CATEGORIA PADRONIZADA],
    SUM(CAST(venda.[Quantidade] AS INT)) AS ALUNOS
FROM {{ source('intel_merc' ,'brz_vendas_historico') }} AS venda
LEFT JOIN {{ source('intel_merc', 'brz_categorizacao_produtos') }} AS categorizacao
  ON venda.[Coleção] COLLATE SQL_Latin1_General_CP1_CI_AS = categorizacao.[COLEÇÃO]
WHERE 
  (
    -- Bloco 1: Coleção PRINCIPAL
    venda.[ANO] = '2023'
    AND venda.[Aluno / Professor] = 'Aluno'
    AND venda.CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
    AND venda.[Tipo de Pedido E/T/A/B/D] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
    AND categorizacao.[CATEGORIA PADRONIZADA] IN ('Coleção', 'Coleção - 1º Semestre', 'Coleção - 2º Semestre')
    AND venda.[Volume] IN ('1', 'Único', 'U')
    AND venda.[Coleção] = 'COLEÇÃO PRINCIPAL'
  )
  OR
  (
    -- Bloco 2: Cambridge
    venda.[ANO] = '2023'
    AND venda.[Aluno / Professor] = 'Aluno'
    AND venda.CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
    AND venda.[Tipo de Pedido E/T/A/B/D] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
    AND venda.[Volume] IN ('1', 'Único', 'U')
    AND venda.[Coleção] = 'CAMBRIDGE'
    AND (
      venda.[Descrição] LIKE '%STUDENT%'
      OR venda.[Descrição] LIKE '%STUDENTS%'
      OR venda.[Descrição] LIKE '%STUDENT''S%'
      OR venda.[Descrição] LIKE '%SB%'
    )
  )
  OR
  (
    -- Bloco 3: Demais coleções exceto PRINCIPAL e CAMBRIDGE
    venda.[ANO] = '2023'
    AND venda.[Aluno / Professor] = 'Aluno'
    AND venda.CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
    AND venda.[Tipo de Pedido E/T/A/B/D] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
    AND venda.[Volume] IN ('1', 'Único', 'U')
    AND venda.[Coleção] NOT IN ('COLEÇÃO PRINCIPAL', 'CAMBRIDGE')
  )
GROUP BY 
    venda.[Código do Cliente],
    venda.[Nome do Cliente],
    venda.[Rede],
    venda.[Cidade],
    venda.[UF],
    venda.[Segmento],
    venda.[Série],
    venda.[Coleção],
    categorizacao.[CATEGORIA PADRONIZADA]