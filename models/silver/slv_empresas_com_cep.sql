SELECT e."accountid",
      e."Nome Fantasia",
      e."Quantidade Total de Alunos",
      e."Rede de Ensino",
      e."Estado",
      e."Cidade",
      e."Bairro",
      e."CEP",
      e."Tipo de Relação",
      e."Área de Atuação",
      e."Código RM",
      e."Código INEP",
      e."Perfil da escola",
      e."CNPJ",
      c."latitude",
      c."longitude",
      c."centroide"
FROM {{ source('intel_merc', 'brz_empresas') }} e
LEFT JOIN {{ source('intel_merc', 'brz_base_cep') }} c
       ON REPLACE(REPLACE(LTRIM(RTRIM(e."CEP")),'-',''),' ','') = 
          REPLACE(REPLACE(LTRIM(RTRIM(c."cep")),'-',''),' ','')