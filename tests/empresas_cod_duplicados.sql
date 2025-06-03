SELECT 
    [Nome Fantasia],
    [Quantidade Total de Alunos],
    [Rede de Ensino],
    [Estado],
    [Cidade],
    [Código RM],
    [Código INEP],
    COUNT(*) as qtd
FROM {{ source ('intel_merc', 'brz_empresas') }}
WHERE [Código RM] IS NOT NULL AND [Código INEP] IS NOT NULL
GROUP BY [Código RM], [Código INEP],  [Nome Fantasia],
    [Quantidade Total de Alunos],
    [Rede de Ensino],
    [Estado],
    [Cidade]
HAVING COUNT(*) > 1