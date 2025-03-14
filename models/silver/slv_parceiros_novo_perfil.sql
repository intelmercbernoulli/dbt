SELECT e.*, s.CO_ENTIDADE,sugestão
FROM {{ source('intel_merc', 'slv_empresas') }} AS e
LEFT JOIN {{ source('intel_merc','slv_escolas_segmentadas') }} AS s
ON e.[Código INEP] = s.[CO_ENTIDADE]
WHERE e.[Tipo de Relação] IN ('Parceiro', 'Novo Parceiro')