SELECT *
FROM {{ source('intel_merc', 'brz_censo_inep_particulares_todos_anos') }}
WHERE NU_ANO_CENSO = 2023
  AND (
      NO_ENTIDADE LIKE '%INTERNACIONAL%'
      OR NO_ENTIDADE LIKE '%INTERNATIONAL%'
      OR NO_ENTIDADE LIKE '%BILINGUE%'
      OR NO_ENTIDADE LIKE '%BILÍNGUE%'
      OR NO_ENTIDADE LIKE '%BILINGUAL%'
      OR NO_ENTIDADE LIKE '%SCHOOL%'
      OR NO_ENTIDADE LIKE '%ENGLISH%'
      OR NO_ENTIDADE LIKE '%INGLES%'
      OR NO_ENTIDADE LIKE '%INGLESA%'
      OR NO_ENTIDADE LIKE '%DUAL LANGUAGE%'
      OR NO_ENTIDADE LIKE '%IMMERSION%'
      OR NO_ENTIDADE LIKE '%INSTITUTE%'
      OR NO_ENTIDADE LIKE '%ACADEMY%'
      OR NO_ENTIDADE LIKE '%COLLEGE%'
      OR NO_ENTIDADE LIKE '%CANADENSE%'
      OR NO_ENTIDADE LIKE '% AMERICAN %'
      OR NO_ENTIDADE LIKE '%BRITISH%'
      OR NO_ENTIDADE LIKE '%GLOBAL%'
      OR NO_ENTIDADE LIKE '%SAINT%'
      OR NO_ENTIDADE LIKE '%COLLEGE%'
  )