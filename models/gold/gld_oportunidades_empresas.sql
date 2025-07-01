with oportunidades as (
    select * 
    from {{ ref('slv_oportunidades_crm') }}
),

empresas as (
    select * 
    from {{ source('intel_merc', 'brz_empresas') }}
)

select 
    *
    
from oportunidades op
left join empresas em
on em.accountid = op.[ID da Escola]