SELECT

    {{ dbt_utils.star(
        from=source('intel_merc', 'brz_registro_de_atendimento'),
        except=[
            'Data de Criação',
            'Hora de Início',
            'Hora de Término'
        ]
    ) }},

    TRY_CONVERT(DATETIME, [Data de Criação], 103)  AS [Data de Criação],
    TRY_CONVERT(DATETIME, [Hora de Início], 103)   AS [Hora de Início],
    TRY_CONVERT(DATETIME, [Hora de Término], 103)  AS [Hora de Término],

    CASE 
        WHEN [Status do Atendimento] = 'Planejado' THEN 0
        WHEN [Status do Atendimento] = 'Agendado' THEN 1
        WHEN [Status do Atendimento] = 'Realizado' THEN 2
        WHEN [Status do Atendimento] = 'Cancelado' THEN 3
        ELSE 4
    END AS prioridade_status_atendimento

FROM {{ source('intel_merc', 'brz_registro_de_atendimento') }}