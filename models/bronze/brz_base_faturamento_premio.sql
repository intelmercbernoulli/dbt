{{ config(materialized='table') }}

SELECT top 10 *
--FROM {{ source('hom_corpore', 'VW_BASE_FATURAMENTO_PREMIO') }}
FROM {{ source('hom_corpore_prod', 'VW_BASE_FATURAMENTO_PREMIO') }}