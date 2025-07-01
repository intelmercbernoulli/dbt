SELECT *
FROM {{ ref('slv_oportunidades_crm') }}
WHERE [Ano de Utilização] = '2.026'