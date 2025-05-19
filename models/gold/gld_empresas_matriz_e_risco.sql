SELECT *, 
CASE 
    WHEN accountid in (
        SELECT distinct(id_account) from {{ source ('intel_merc', 'brz_mapa_de_risco') }} ) 
        THEN 'Sim'
    ELSE 'Não'
END AS [Escola Mapeada]
FROM
{{ ref ('slv_empresas_crm') }}