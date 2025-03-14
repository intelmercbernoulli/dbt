SELECT *
FROM
    {{ ref('slv_cubo33') }}
WHERE
    [ANO DE UTILIZAÇÃO] = '2024'
    AND [ALUNO/PROFESSOR] = 'Aluno'
    AND CODTMV NOT IN ('2.1.48', '2.2.76', '2.1.04')
    AND [TIPO DE PEDIDO] IN ('Acerto de NF', 'Adicional', 'D', 'Tiragem')
    AND NOT ANO = '2025'