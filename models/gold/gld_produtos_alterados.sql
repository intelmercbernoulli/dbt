WITH repetidos AS (
    SELECT [cod_coligada_produto]
    FROM {{ ref('alteracao_de_produto') }}
    GROUP BY [cod_coligada_produto]
    HAVING COUNT(*) > 1
)
SELECT s.*
FROM {{ ref('alteracao_de_produto') }} AS s
INNER JOIN repetidos r
    ON s.[cod_coligada_produto] = r.[cod_coligada_produto];