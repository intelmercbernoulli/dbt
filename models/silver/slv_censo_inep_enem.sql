SELECT 
    escola.[CO_ENTIDADE],
    escola.[NO_ENTIDADE],
    escola.[NO_MUNICIPIO],
    escola.[CO_MUNICIPIO],
    escola.[SG_UF],
    escola.[QT_MAT_BAS],
    enem.[nota_enem],

    -- Ranking por quantidade de matrículas (sempre presente)
    RANK() OVER (
        PARTITION BY escola.[CO_MUNICIPIO]
        ORDER BY escola.[QT_MAT_BAS] DESC
    ) AS rank_qt_mat_bas,

    -- Ranking por nota do ENEM (só se a nota não for nula)
    CASE 
        WHEN enem.[nota_enem] IS NOT NULL THEN
            RANK() OVER (
                PARTITION BY escola.[CO_MUNICIPIO]
                ORDER BY enem.[nota_enem] DESC
            )
        ELSE NULL
    END AS rank_nota_enem

FROM {{ source('intel_merc','brz_censo_inep_2024') }} AS escola
LEFT JOIN {{ source('intel_merc','brz_enem_escolas') }} AS enem
    ON escola.[CO_ENTIDADE] = enem.[CO_ESCOLA]
    AND enem.[ano] = 2023