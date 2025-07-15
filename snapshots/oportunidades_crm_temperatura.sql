{% snapshot oportunidades_crm_temperatura %}
    {{
        config(
          target_schema='snapshots',
          unique_key='id_oportunidade',
          strategy='check',
          check_cols=['status_negociacao', 'fase_venda', 'temperatura']
        )
    }}

SELECT
  [Id Oportunidade] AS id_oportunidade,
 [Status da negociação] AS status_negociacao,
 [Fase da Venda] AS fase_venda,
 [Temperatura do Cliente - Comercial] AS temperatura
 FROM {{ ref('gld_oportunidades_crm_2026') }}
{% endsnapshot %}