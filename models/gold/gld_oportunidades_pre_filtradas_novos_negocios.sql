SELECT
    [ID da Escola],
    [Nome Fantasia],
    [Id Oportunidade],
    [Negociação Referente a],
    [Oportunidade Referente a],
    [Status da negociação],
    [Fase da Venda],
    [Data de Fechamento do Contrato],
    [Tempo de Contrato],
    TRY_CAST(REPLACE([Ano de Utilização], '.', '') AS INT) - 1  AS [Ano de Fechamento],
    [Ano de Utilização],
    [Receita Bruta],
    [Receita Consultor],
    [Quantidade de Alunos Negociados],
    [Estado],
    [Cidade],
    [Perfil da escola]
FROM {{ ref('gld_oportunidades_empresas') }}
WHERE [Negociação Referente a] = 'Comercial - Bernoulli'
    AND [Status da negociação] = 'Contrato / Termo assinado'
    AND [Ano de Utilização] IN ('2.024','2.025','2.026','2.027')