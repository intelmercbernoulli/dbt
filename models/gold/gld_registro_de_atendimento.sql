SELECT *, 
CASE 
    WHEN [Status do Atendimento] = 'Planejado' THEN 0
    WHEN [Status do Atendimento]  = 'Agendado' THEN 1
    WHEN [Status do Atendimento]  = 'Realizado' THEN 2
    WHEN [Status do Atendimento]  = 'Cancelado' THEN 3
    ELSE 4
END AS prioridade_status_atendimento
FROM
{{ source ('intel_merc', 'brz_registro_de_atendimento') }}
-- WHERE [Setor Relacionado] = 'Experiência do Cliente'