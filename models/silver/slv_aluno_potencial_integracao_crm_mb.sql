WITH base AS (
    SELECT
        [id_account],
        [Série]
    FROM {{ source ('intel_merc', 'brz_aluno_potencial') }}
    WHERE [Quantidade Alunos] != '0'
      AND [Quantidade Alunos] IS NOT NULL
      AND Interesse = 'Sim'
)
SELECT
    [id_account],
    CASE WHEN [EI 2 anos] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EI 2 anos],
    CASE WHEN [EI 3 anos] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EI 3 anos],
    CASE WHEN [EI 4 anos] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EI 4 anos],
    CASE WHEN [EI 5 anos] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EI 5 anos],
    CASE WHEN [EFAI 1º ano] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EFAI 1º ano],
    CASE WHEN [EFAI 2º ano] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EFAI 2º ano],
    CASE WHEN [EFAI 3º ano] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EFAI 3º ano],
    CASE WHEN [EFAI 4º ano] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EFAI 4º ano],
    CASE WHEN [EFAI 5º ano] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EFAI 5º ano],
    CASE WHEN [EFAF 6º ano] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EFAF 6º ano],
    CASE WHEN [EFAF 7º ano] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EFAF 7º ano],
    CASE WHEN [EFAF 8º ano] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EFAF 8º ano],
    CASE WHEN [EFAF 9º ano] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EFAF 9º ano],
    CASE WHEN [EM 1ª série] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EM 1ª série],
    CASE WHEN [EM 2ª série] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EM 2ª série],
    CASE WHEN [EM 3ª série] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [EM 3ª série],
    CASE WHEN [Pré-vestibular anual] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [Pré-vestibular anual],
    CASE WHEN [Pré-vestibular semestral] IS NOT NULL THEN 'Sim' ELSE 'Não' END AS [Pré-vestibular semestral]
FROM base
PIVOT (
    MAX([Série]) FOR [Série] IN (
        [EI 2 anos],
        [EI 3 anos],
        [EI 4 anos],
        [EI 5 anos],
        [EFAI 1º ano],
        [EFAI 2º ano],
        [EFAI 3º ano],
        [EFAI 4º ano],
        [EFAI 5º ano],
        [EFAF 6º ano],
        [EFAF 7º ano],
        [EFAF 8º ano],
        [EFAF 9º ano],
        [EM 1ª série],
        [EM 2ª série],
        [EM 3ª série],
        [Pré-vestibular anual],
        [Pré-vestibular semestral]
    )
) AS pvt;
