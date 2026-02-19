SELECT *
FROM {{ ref('slv_registro_de_atendimento_crm') }}
WHERE [Setor Relacionado] = 'Experiência do Cliente'