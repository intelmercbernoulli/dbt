WITH base AS (
    SELECT
        *
    FROM  {{ source('intel_merc', 'brz_aluno_potencial') }}
    WHERE [Contrato Atual] IS NOT NULL
      AND [Status] = 'Ativo(a)'
),
ordenado AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id_account ORDER BY [Data de Criação] DESC) AS rn
    FROM base
)
SELECT
    [id_account],
    [Contrato Atual]
    -- Adicione aqui outras colunas que você deseja manter
FROM ordenado
WHERE rn = 1
  AND [id_account] IS NOT NULL
  AND [id_account] <> '';
