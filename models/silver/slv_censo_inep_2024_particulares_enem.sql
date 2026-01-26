SELECT c.[NU_ANO_CENSO],
      c.[CO_ENTIDADE],
      c.[NO_ENTIDADE],
      c.[NO_MUNICIPIO],
      c.[NO_REGIAO],
      c.[SG_UF],
      c.[QT_MAT_BAS],
      c.[QT_MAT_INF],
      c.[QT_MAT_FUND_AI],
      c.[QT_MAT_FUND_AF],
      c.[QT_MAT_MED],
      e.[nota_enem],
      e.[alunos]
FROM {{ source('intel_merc', 'brz_censo_inep_2024') }}  c
LEFT JOIN {{ source('intel_merc', 'brz_enem_2024') }}  e
       ON c.[CO_ENTIDADE] = e.[CO_ESCOLA]
WHERE c.[TP_DEPENDENCIA] = 4
  AND c.[TP_SITUACAO_FUNCIONAMENTO] = 1
  AND c.[IN_REGULAR] = 1;
