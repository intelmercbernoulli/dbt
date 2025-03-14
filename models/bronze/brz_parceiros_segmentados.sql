SELECT e.*, s.CO_ENTIDADE,sugestão
FROM [DB_DLK_INTMERCDB].[dbo].[slv_empresas] AS e
LEFT JOIN [dbo].[slv_escolas_segmentadas] AS s
ON e.[Código INEP] = s.[CO_ENTIDADE]
WHERE e.[Tipo de Relação] IN ('Parceiro', 'Novo Parceiro')