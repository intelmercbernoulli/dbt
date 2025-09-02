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
END AS [Rede],
CASE 
    WHEN CAST([Quantidade Total de Alunos] AS INT) > 1000 THEN 'e.> 1.000'
    WHEN CAST([Quantidade Total de Alunos] AS INT) > 750 THEN 'd. <= 1.000'
    WHEN CAST([Quantidade Total de Alunos] AS INT) > 500 THEN 'c. <= 750'
    WHEN CAST([Quantidade Total de Alunos] AS INT) > 250 THEN 'b. <= 500'
    WHEN CAST([Quantidade Total de Alunos] AS INT) <= 250 THEN 'a. <= 250'
    ELSE '-'
END AS [Porte],
CASE 
    WHEN [Estado] IN ('Acre','Amapá','Amazonas','Pará','Rondônia','Roraima','Tocantins') THEN 'Norte'
    WHEN [Estado] IN ('Alagoas','Bahia','Ceará','Maranhão','Paraíba','Pernambuco','Piaui','Rio Grande do Norte','Sergipe') THEN 'Nordeste'
    WHEN [Estado] IN ('Distrito Federal','Goiás','Mato Grosso','Mato Grosso do Sul') THEN 'Centro-Oeste'
    WHEN [Estado] IN ('Espírito Santo','Minas Gerais','Rio de Janeiro','São Paulo') THEN 'Sudeste'
    WHEN [Estado] IN ('Paraná','Santa Catarina','Rio Grande do Sul') THEN 'Sul'
    ELSE '-'
END AS [Região]
FROM
{{ source ('intel_merc', 'brz_empresas') }}
WHERE [Status] = 'Ativa'