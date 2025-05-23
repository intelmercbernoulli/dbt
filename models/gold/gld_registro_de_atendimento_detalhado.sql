SELECT id_account, activityid, 'Feedback' AS Tipo, LTRIM(RTRIM(value)) AS Desdobramento
FROM {{ source ('intel_merc', 'brz_registro_de_atendimento') }}
CROSS APPLY STRING_SPLIT([Desdobramento da Ação - Feedback], ';')

UNION ALL

SELECT id_account, activityid, 'Formação' AS Tipo, LTRIM(RTRIM(value)) AS Desdobramento
FROM {{ source ('intel_merc', 'brz_registro_de_atendimento') }}
CROSS APPLY STRING_SPLIT([Desdobramento da Ação - Formação], ';')

UNION ALL

SELECT id_account, activityid, 'Implantação' AS Tipo, LTRIM(RTRIM(value)) AS Desdobramento
FROM {{ source ('intel_merc', 'brz_registro_de_atendimento') }}
CROSS APPLY STRING_SPLIT([Desdobramento da Ação - Implantação], ';')

UNION ALL

SELECT id_account, activityid, 'Reunião' AS Tipo, LTRIM(RTRIM(value)) AS Desdobramento
FROM {{ source ('intel_merc', 'brz_registro_de_atendimento') }}
CROSS APPLY STRING_SPLIT([Desdobramento da Ação - Reunião], ';')