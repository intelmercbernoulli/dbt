SELECT
    [id_account],
    [Cliente],
    SUBSTRING([Data do Atendimento], 7, 4) AS [Ano de Atendimento],
    COUNT(*) AS Qtde_Atendimentos
FROM {{ source('intel_merc', 'brz_solicitacoes_crm') }}
WHERE [Status do Agendamento] = 'Realizado pelo CO'
GROUP BY
    [id_account],
    [Cliente],
    SUBSTRING([Data do Atendimento], 7, 4);