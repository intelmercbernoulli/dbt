{% snapshot alteracao_de_produto %}
  {{
    config(
      target_schema='snapshots',
      unique_key='cod_coligada_produto',
      strategy='check',
      check_cols=[
        'PRODUTOPROTHEUS','DESCRIÇÃO','DESCRIÇÃO AUXILIAR','CODIGO DE BARRAS','ATIVO / INATIVO',
        'IDPRD','COLEÇÃO/SAEP','CATEGORIA','VOLUME','ANO','ALUNO/PROFESSOR','PROVA','TIPO DE PERSONALIZAÇÃO',
        'SEGMENTO','SERIE','APLICAÇÃO','FORMATO','DISCIPLINA','TIPO DE MATERIAL'
        ]
    )
  }}

SELECT
      [COLIGADA] + [CÓDIGO PRODUTO] AS [cod_coligada_produto]
      ,[COLIGADA]
      ,[CÓDIGO PRODUTO]
      ,[PRODUTOPROTHEUS]
      ,[DESCRIÇÃO]
      ,[DESCRIÇÃO AUXILIAR]
      ,[CODIGO DE BARRAS]
      ,[ATIVO / INATIVO]
      ,[IDPRD]
      ,[COLEÇÃO/SAEP]
      ,[CATEGORIA]
      ,[VOLUME]
      ,[ANO]
      ,[ALUNO/PROFESSOR]
      ,[PROVA]
      ,[TIPO DE PERSONALIZAÇÃO]
      ,[SEGMENTO]
      ,[SERIE]
      ,[APLICAÇÃO]
      ,[FORMATO]
      ,[DISCIPLINA]
      ,[TIPO DE MATERIAL]
  FROM {{ source('intel_merc','brz_conferenciaprodutos_rm') }}

  {% endsnapshot %}