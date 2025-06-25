SELECT [Nome Fantasia]
      ,[Estado]
      ,[Cidade]
      ,[Tipo de Relação]
      ,[Escola em Serviço?]
      ,[Gerência Pedagógica Atual]
      ,[Consultor Pedagógico Atual]
      ,[Consultor Bilíngue Atual]
  FROM {{ source('intel_merc', 'brz_empresas') }}
  WHERE [Tipo de Relação] IN ('BSE - Novo Parceiro','BSE - Parceiro','MULTI - Novo Parceiro','MULTI - Parceiro')
  AND (
	[Gerência Pedagógica Atual] IS NULL
      OR [Consultor Pedagógico Atual] IS NULL
      AND [Consultor Bilíngue Atual] IS NOT NULL)