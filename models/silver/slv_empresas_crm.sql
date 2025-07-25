SELECT *,
RIGHT('000000' + LEFT(CAST([Código RM] AS VARCHAR), CHARINDEX('.', CAST([Código RM] AS VARCHAR) + '.') - 1), 6) AS [Cod RM Ajustado],
RIGHT('000000' + LEFT(CAST([Código 1] AS VARCHAR), CHARINDEX('.', CAST([Código 1] AS VARCHAR) + '.') - 1), 6) AS [Cod RM 1 Ajustado],
CASE 
    WHEN [Estado] = 'Pará' THEN 'PA'
    WHEN [Estado] = 'Paraíba' THEN 'PB'
    WHEN [Estado] = 'Distrito Federal' THEN 'DF'
    WHEN [Estado] = 'Sergipe' THEN 'SE'
    WHEN [Estado] = 'Espírito Santo' THEN 'ES'
    WHEN [Estado] = 'Rondônia' THEN 'RO'
    WHEN [Estado] = 'Acre' THEN 'AC'
    WHEN [Estado] = 'Paraná' THEN 'PR'
    WHEN [Estado] = 'Rio de Janeiro' THEN 'RJ'
    WHEN [Estado] = 'Mato Grosso do Sul' THEN 'MS'
    WHEN [Estado] = 'Alagoas' THEN 'AL'
    WHEN [Estado] = 'Mato Grosso' THEN 'MT'
    WHEN [Estado] = 'Santa Catarina' THEN 'SC'
    WHEN [Estado] = 'Pernambuco' THEN 'PE'
    WHEN [Estado] = 'Piaui' THEN 'PI'
    WHEN [Estado] = 'Rio Grande do Sul' THEN 'RS'
    WHEN [Estado] = 'Bahia' THEN 'BA'
    WHEN [Estado] = 'Gifu' THEN 'GI'
    WHEN [Estado] = 'Tocantins' THEN 'TO'
    WHEN [Estado] = 'Goiás' THEN 'GO'
    WHEN [Estado] = 'Ceará' THEN 'CE'
    WHEN [Estado] = 'Maranhão' THEN 'MA'
    WHEN [Estado] = 'Shizuoka' THEN 'SH'
    WHEN [Estado] = 'Amazonas' THEN 'AM'
    WHEN [Estado] = 'Minas Gerais' THEN 'MG'
    WHEN [Estado] = 'Amapá' THEN 'AP'
    WHEN [Estado] = 'São Paulo' THEN 'SP'
    WHEN [Estado] = 'Roraima' THEN 'RR'
    WHEN [Estado] = 'Rio Grande do Norte' THEN 'RN'
    ELSE '-'  -- Caso algum estado não esteja mapeado
END AS [UF],
CASE 
    WHEN [Rede de Ensino] IS NULL THEN 'Sem Rede'
    ELSE [Rede de Ensino] 
END AS [Rede]
FROM
{{ source ('intel_merc', 'brz_empresas') }}