{{ config(
    materialized = 'table'
) }}

WITH beneficios AS (

    SELECT
        FC.CODCFO                                  AS codigo,

        CASE
            WHEN BN.TIPO = 1 THEN 'Desconto'
            WHEN BN.TIPO = 2 THEN 'Bonificação'
            ELSE 'Frete'
        END                                         AS tipo,

        BN.PERCENTUAL                               AS percentual,
        BN.VALIDADE                                 AS validade,
        BN.FAIXAMIN                                 AS faixa_minima,
        BN.FAIXAMAX                                 AS faixa_maxima,

        GC_TP.DESCRICAO                             AS tipo_pedido,
        GC_CAT.DESCRICAO                            AS categoria,
        GC_SEG.DESCRICAO                            AS segmento,
        GC_DIS.DESCRICAO                            AS disciplina,
        GC_COL.DESCRICAO                            AS colecao,
        GC_VOL.DESCRICAO                            AS volume,
        GC_ANO.DESCRICAO                            AS ano

    FROM {{ source('hom_corpore', 'FCFO') }} FC

    JOIN {{ source('hom_corpore', 'ZMDBENEFICIOSCNT') }} BN
        ON FC.CODCFO = BN.CODCFO

    LEFT JOIN {{ source('hom_corpore', 'GCONSIST') }} GC_TP
        ON GC_TP.CODCLIENTE = BN.TIPOPEDIDO
       AND GC_TP.CODTABELA = 'TIPOPEDIDO'
       AND GC_TP.CODCOLIGADA = 0

    LEFT JOIN {{ source('hom_corpore', 'GCONSIST') }} GC_CAT
        ON GC_CAT.CODCLIENTE = BN.CATEGORIA
       AND GC_CAT.CODTABELA = 'CATEGORIA'
       AND GC_CAT.CODCOLIGADA = 0

    LEFT JOIN {{ source('hom_corpore', 'GCONSIST') }} GC_SEG
        ON GC_SEG.CODCLIENTE = BN.SEGMENTO
       AND GC_SEG.CODTABELA = 'SEGMENTO'
       AND GC_SEG.CODCOLIGADA = 0

    LEFT JOIN {{ source('hom_corpore', 'GCONSIST') }} GC_DIS
        ON GC_DIS.CODCLIENTE = BN.DISCIPLINA
       AND GC_DIS.CODTABELA = 'DIS'
       AND GC_DIS.CODCOLIGADA = 0

    LEFT JOIN {{ source('hom_corpore', 'GCONSIST') }} GC_COL
        ON GC_COL.CODCLIENTE = BN.TIPOCOLECAO
       AND GC_COL.CODTABELA = 'COL'
       AND GC_COL.CODCOLIGADA = 0

    LEFT JOIN {{ source('hom_corpore', 'GCONSIST') }} GC_VOL
        ON GC_VOL.CODCLIENTE = BN.VOLUME
       AND GC_VOL.CODTABELA = 'VOL'
       AND GC_VOL.CODCOLIGADA = 0

    LEFT JOIN {{ source('hom_corpore', 'GCONSIST') }} GC_ANO
        ON GC_ANO.CODCLIENTE = BN.ANO
       AND GC_ANO.CODTABELA = 'ANO'
       AND GC_ANO.CODCOLIGADA = 0

)

SELECT *
FROM beneficios