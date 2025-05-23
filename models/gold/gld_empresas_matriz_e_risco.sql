SELECT *, 
CASE 
    WHEN accountid in (
        SELECT distinct(id_account) from {{ source ('intel_merc', 'brz_mapa_de_risco') }} ) 
        THEN 'Sim'
    ELSE 'Não'
END AS [Escola Mapeada],
CASE 
    WHEN accountid in (
        SELECT distinct(id_account) from {{ source ('intel_merc', 'brz_registro_de_atendimento') }} ) 
        THEN 'Sim'
    ELSE 'Não'
END AS [Escola Atendida]
FROM
{{ ref ('slv_empresas_crm') }}
WHERE [Escola em Serviço?] IS NOT NULL
  AND [Escola em Serviço?] <> 'Não'