{{ config(materialized='table') }}

SELECT *
FROM {{ source('operacoes', 'BI_VENDACOLECAO_MULTITUDE') }}