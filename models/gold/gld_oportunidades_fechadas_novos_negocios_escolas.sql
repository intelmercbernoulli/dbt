SELECT
    [ID da Escola],
    [Ano de Fechamento],
    SUM([Receita Bruta]) receita_bruta,
    SUM([Receita Consultor]) receita_liquida,
    SUM([Quantidade de Alunos Negociados]) alunos_fechados
FROM {{ ref('gld_oportunidades_pre_filtradas_novos_negocios') }}
GROUP BY [ID da Escola], [Ano de Fechamento]