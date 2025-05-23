{% snapshot empresas_snapshot %}
  {{
    config(
      target_schema='snapshots',
      unique_key='accountid',
      strategy='check',
      check_cols=[
        'area_de_atuacao',
        'gerencia_pedagogica_atual',
        'consultor_pedagogico_atual',
        'proprietario',
        'lideranca_regional',
        'lideranca_nacional'
]
    )
  }}

  SELECT
  accountid,
  [Área de Atuação] AS area_de_atuacao,
  [Gerência Pedagógica Atual] AS gerencia_pedagogica_atual,
  [Consultor Pedagógico Atual] AS consultor_pedagogico_atual,
  [Proprietário] AS proprietario,
  [Liderança Regional] AS lideranca_regional,
  [Liderança Nacional] AS lideranca_nacional
FROM {{ source('intel_merc','brz_empresas') }}
{% endsnapshot %}