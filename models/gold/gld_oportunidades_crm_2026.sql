SELECT 
*,
[Id Oportunidade] AS id_oportunidade
FROM   {{ source('intel_merc', 'brz_oportunidades') }}
WHERE [Ano de Utilização] = '2.026'