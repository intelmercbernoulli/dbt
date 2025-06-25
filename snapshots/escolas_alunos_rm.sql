{% snapshot escolas_alunos_rm %}
    {{
        config(
          target_schema='snapshots',
          unique_key="cod_cli COLLATE SQL_Latin1_General_CP1_CI_AS + serie COLLATE SQL_Latin1_General_CP1_CI_AS",
          strategy='check',
          check_cols=['ALUNOS']
        )
    }}

SELECT
  [CÓDIGO DO CLIENTE] AS cod_cli,
  [NOME DO CLIENTE],
  [REDE DE ENSINO],
  [CIDADE],
  [UF],
  [SEGMENTO],
  [SÉRIE] AS serie,
  [GRUPO COLEÇÃO],
  [CATEGORIA PADRONIZADA],
  [ALUNOS]
FROM {{ ref('gld_escolas_alunos_colecao_principal_ano_atual') }}
{% endsnapshot %}