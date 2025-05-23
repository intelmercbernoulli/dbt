{% snapshot oportunidades_snapshot %}
    {{
        config(
          target_schema='snapshots',
          unique_key='id_oportunidade',
          strategy='check',
          check_cols=['status_negociacao', 'fase_venda']
        )
    }}

SELECT
 id_oportunidade,
 [Status da negociação] AS status_negociacao,
 [Fase da Venda] AS fase_venda
 FROM {{ ref('gld_oportunidades_crm_2026') }}
{% endsnapshot %}