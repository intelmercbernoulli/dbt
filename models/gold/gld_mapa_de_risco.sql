SELECT *, 
CASE 
    WHEN [Nível de Risco] = 'Alto' THEN 0
    WHEN [Nível de Risco]  = 'Médio' THEN 1
    WHEN [Nível de Risco]  = 'Baixo' THEN 2
END AS prioridade_nivel_risco
FROM
{{ source ('intel_merc', 'brz_mapa_de_risco') }}