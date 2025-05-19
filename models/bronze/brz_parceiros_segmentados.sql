SELECT e.*, s.CO_ENTIDADE,sugestão
FROM [DB_DLK_INTMERCDB].[dbo].[brz_empresas] AS e
LEFT JOIN [dbo].[brz_segmentacao_escolas] AS s
ON e.[Código INEP] = s.[CO_ENTIDADE]
WHERE e.[Tipo de Relação] IN ('Parceiro', 'Novo Parceiro')