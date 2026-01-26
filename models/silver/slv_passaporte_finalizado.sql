SELECT [ID do Pedido],
       [Solicitante],
       [id_account],
       [Cliente],
       [Assunto],
       [Responsável pelo atendimento],
       [Status de Solicitação],
       [Etapa da Solicitação],
       [Aprovado pela Gerência],
       [Status do Agendamento],
       [Tipo de Atendimento],
       [Assunto Principal],
       [Data de Criação],
       [Data de Solicitação],
       [Data de Expedição],
       [Previsão de Entrega],
       [Data de Entrega],
       [Previsão de Conclusão da Análise],
       [Data de Conclusão],
       [Data de Cancelamento],
       [Data do Atendimento],
       [Data de Atendimento],
       [Início da Programação],
       [Término da Programação],
       [Participante BSE 1],
       [Participante BSE 2],
       [Participante BSE 3],
       RIGHT([Término da Programação], 4) AS [Ano do Término da Programação],
       DATEPART(WEEK, TRY_CONVERT(date, [Término da Programação])) AS [Semana do término da programação]
FROM {{source('intel_merc', 'brz_solicitacoes_crm') }}
WHERE [Assunto] = 'CO - Passaporte Bernoulli'
  AND [Etapa da Solicitação] IS NOT NULL
  AND [Etapa da Solicitação] NOT IN (
        'Passaporte Cancelado',
        'Passaporte Solicitado'
      )