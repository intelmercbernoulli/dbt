SELECT 
    atual.cod_cli,
    atual.[NOME DO CLIENTE],
    atual.CIDADE,
    atual.UF,
    atual.SEGMENTO,
    atual.serie,
    atual.ALUNOS AS aluno_atual,
    ISNULL(antigo.ALUNOS, atual.ALUNOS) AS aluno_antigo
FROM 
    {{ ref('escolas_alunos_rm') }} AS atual
LEFT JOIN (
    SELECT 
        cod_cli, 
        serie, 
        MAX(dbt_valid_to) AS max_valid_to
    FROM {{ ref('escolas_alunos_rm') }} 
    WHERE dbt_valid_to IS NOT NULL
    GROUP BY cod_cli, serie
) AS ult_data ON atual.cod_cli = ult_data.cod_cli AND atual.serie = ult_data.serie
LEFT JOIN {{ ref('escolas_alunos_rm') }}  AS antigo
    ON antigo.cod_cli = ult_data.cod_cli 
    AND antigo.serie = ult_data.serie
    AND antigo.dbt_valid_to = ult_data.max_valid_to
WHERE atual.dbt_valid_to IS NULL;