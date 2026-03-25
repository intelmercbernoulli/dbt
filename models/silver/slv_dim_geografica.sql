WITH base AS (

    SELECT DISTINCT
        UF,
        Cidade
    FROM {{ source('intel_merc', 'brz_pedidos_raizes') }}

),

enriched AS (

    SELECT
        UF,
        Cidade,

        -- Região
        CASE 
            WHEN UF IN ('RS','SC','PR') THEN 'Sul'
            WHEN UF IN ('SP','RJ','MG','ES') THEN 'Sudeste'
            WHEN UF IN ('MT','MS','GO','DF') THEN 'Centro-Oeste'
            WHEN UF IN ('BA','SE','AL','PE','PB','RN','CE','PI','MA') THEN 'Nordeste'
            WHEN UF IN ('AM','PA','AC','RO','RR','AP','TO') THEN 'Norte'
            ELSE 'Não definido'
        END AS Regiao,

        -- Flag capital (correto com UF)
        CASE 
            WHEN (Cidade = 'Porto Alegre' AND UF = 'RS') THEN 1
            WHEN (Cidade = 'Florianópolis' AND UF = 'SC') THEN 1
            WHEN (Cidade = 'Curitiba' AND UF = 'PR') THEN 1
            WHEN (Cidade = 'São Paulo' AND UF = 'SP') THEN 1
            WHEN (Cidade = 'Rio de Janeiro' AND UF = 'RJ') THEN 1
            WHEN (Cidade = 'Belo Horizonte' AND UF = 'MG') THEN 1
            WHEN (Cidade = 'Vitória' AND UF = 'ES') THEN 1
            WHEN (Cidade = 'Salvador' AND UF = 'BA') THEN 1
            WHEN (Cidade = 'Aracaju' AND UF = 'SE') THEN 1
            WHEN (Cidade = 'Maceió' AND UF = 'AL') THEN 1
            WHEN (Cidade = 'Recife' AND UF = 'PE') THEN 1
            WHEN (Cidade = 'João Pessoa' AND UF = 'PB') THEN 1
            WHEN (Cidade = 'Natal' AND UF = 'RN') THEN 1
            WHEN (Cidade = 'Fortaleza' AND UF = 'CE') THEN 1
            WHEN (Cidade = 'Teresina' AND UF = 'PI') THEN 1
            WHEN (Cidade = 'São Luís' AND UF = 'MA') THEN 1
            WHEN (Cidade = 'Cuiabá' AND UF = 'MT') THEN 1
            WHEN (Cidade = 'Campo Grande' AND UF = 'MS') THEN 1
            WHEN (Cidade = 'Goiânia' AND UF = 'GO') THEN 1
            WHEN (Cidade = 'Brasília' AND UF = 'DF') THEN 1
            WHEN (Cidade = 'Manaus' AND UF = 'AM') THEN 1
            WHEN (Cidade = 'Belém' AND UF = 'PA') THEN 1
            WHEN (Cidade = 'Rio Branco' AND UF = 'AC') THEN 1
            WHEN (Cidade = 'Porto Velho' AND UF = 'RO') THEN 1
            WHEN (Cidade = 'Boa Vista' AND UF = 'RR') THEN 1
            WHEN (Cidade = 'Macapá' AND UF = 'AP') THEN 1
            WHEN (Cidade = 'Palmas' AND UF = 'TO') THEN 1
            ELSE 0
        END AS IsCapital

    FROM base

),

final AS (

    SELECT
        UF,
        Cidade,
        Regiao,
        IsCapital,

        -- Localidade
        CASE 
            WHEN IsCapital = 1 THEN CONCAT(Regiao, ' Capital')
            ELSE CONCAT(Regiao, ' Interior')
        END AS Localidade,

        -- SLA de entrega
        CASE 
            WHEN Regiao = 'Centro-Oeste' AND IsCapital = 0 THEN 11
            WHEN Regiao = 'Centro-Oeste' AND IsCapital = 1 THEN 9

            WHEN Regiao = 'Nordeste' AND IsCapital = 0 THEN 15
            WHEN Regiao = 'Nordeste' AND IsCapital = 1 THEN 10

            WHEN Regiao = 'Norte' AND IsCapital = 0 THEN 21
            WHEN Regiao = 'Norte' AND IsCapital = 1 THEN 18

            WHEN Regiao = 'Sudeste' AND IsCapital = 0 THEN 10
            WHEN Regiao = 'Sudeste' AND IsCapital = 1 THEN 8

            WHEN Regiao = 'Sul' AND IsCapital = 0 THEN 10
            WHEN Regiao = 'Sul' AND IsCapital = 1 THEN 8

            ELSE NULL
        END AS DiasParaEntrega

    FROM enriched

)

SELECT * FROM final