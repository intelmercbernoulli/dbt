SELECT *
FROM {{ ref('slv_empresas_crm') }}
WHERE [Tipo de Relação] IN ('BSE - Em Processo de Rescisão','BSE - Ex-Cliente')