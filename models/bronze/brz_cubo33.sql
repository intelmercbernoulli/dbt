--{{ config(materialized='table') }}

SELECT *
--FROM {{ source('operacoes', 'BI_VENDACOLECAO') }}
FROM {{ source('intel_merc', 'brz_vendacolecao_rm') }}