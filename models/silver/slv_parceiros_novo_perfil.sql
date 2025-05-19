SELECT e.*, s.CO_ENTIDADE,sugestão
FROM {{ source('intel_merc', 'brz_empresas') }} AS e
LEFT JOIN {{ source('intel_merc','brz_segmentacao_escolas') }} AS s
ON e.[Código INEP] = s.[CO_ENTIDADE]
WHERE e.[Tipo de Relação] IN ('Parceiro', 'Novo Parceiro')