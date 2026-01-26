SELECT DISTINCT
    c.CO_ENTIDADE,
    c.NO_ENTIDADE,
    c.SG_UF,
    c.NO_MUNICIPIO,
    e.ano AS [Ano ENEM],
    e.NOTA_CH,
    e.NOTA_CN,
    e.NOTA_LC,
    e.NOTA_MT,
    e.NU_NOTA_REDACAO,
    e.nota_enem,
    crm.[Tipo de Relação],
    crm.[Início da Utilização],
    crm.[Ano da Rescisão]
FROM {{ source('intel_merc', 'brz_censo_inep_2024') }} c
LEFT JOIN {{ source('intel_merc', 'brz_enem_escolas') }} e
    ON c.CO_ENTIDADE = e.CO_ESCOLA
LEFT JOIN {{ ref('slv_empresas_crm') }} crm
    ON c.CO_ENTIDADE = crm.[Código INEP]
WHERE c.TP_DEPENDENCIA = 4
  AND c.IN_REGULAR = 1
  AND c.TP_SITUACAO_FUNCIONAMENTO = 1
  AND e.nota_enem IS NOT NULL
