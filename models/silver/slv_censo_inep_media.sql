WITH medias_municipio AS (
    SELECT
        CO_MUNICIPIO,
        NU_ANO_CENSO,
        CAST(AVG(CAST(QT_MAT_BAS AS INT)) AS INT) AS MEDIA_QT_MAT_BAS,
        CAST(AVG(CAST(QT_MAT_INF AS INT)) AS INT) AS MEDIA_QT_MAT_INF,
        CAST(AVG(CAST(QT_MAT_FUND_AI AS INT)) AS INT) AS MEDIA_QT_MAT_FUND_AI,
        CAST(AVG(CAST(QT_MAT_FUND_AF AS INT)) AS INT) AS MEDIA_QT_MAT_FUND_AF,
        CAST(AVG(CAST(QT_MAT_MED AS INT)) AS INT) AS MEDIA_QT_MAT_MED
    FROM
        {{ source('intel_merc','brz_censo_inep_particulares_todos_anos') }}
    WHERE [TP_SITUACAO_FUNCIONAMENTO] = '1' AND [IN_REGULAR] = '1'
    GROUP BY
        CO_MUNICIPIO, NU_ANO_CENSO
)

SELECT
    c.NU_ANO_CENSO,
    c.CO_ENTIDADE,
    c.NO_ENTIDADE,
    c.CO_MUNICIPIO,
    c.QT_MAT_BAS,
    c.QT_MAT_INF,
    c.QT_MAT_FUND_AI,
    c.QT_MAT_FUND_AF,
    c.QT_MAT_MED,
    m.MEDIA_QT_MAT_BAS,
    m.MEDIA_QT_MAT_INF,
    m.MEDIA_QT_MAT_FUND_AI,
    m.MEDIA_QT_MAT_FUND_AF,
    m.MEDIA_QT_MAT_MED

FROM
    {{ source('intel_merc','brz_censo_inep_particulares_todos_anos') }} c
LEFT JOIN
    medias_municipio m
    ON c.CO_MUNICIPIO = m.CO_MUNICIPIO
    AND c.NU_ANO_CENSO = m.NU_ANO_CENSO