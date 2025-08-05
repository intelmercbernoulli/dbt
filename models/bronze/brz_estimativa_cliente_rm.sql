{{ config(materialized='table') }}

SELECT *
FROM {{ source('hom_corpore', 'VW_BI_ESTIMATIVA') }}