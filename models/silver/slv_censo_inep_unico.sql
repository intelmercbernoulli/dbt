SELECT a.CO_ENTIDADE
FROM {{ source('intel_merc', 'brz_censo_inep_2024') }} AS a
LEFT JOIN {{ source('intel_merc', 'brz_censo_inep_2023') }} AS b
    ON a.CO_ENTIDADE = b.CO_ENTIDADE
WHERE b.CO_ENTIDADE IS NULL;