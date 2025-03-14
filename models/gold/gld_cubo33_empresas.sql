SELECT 
    e.*, 
    s.[Código RM],
    RIGHT(REPLICATE('0', 6) + LEFT(s.[Código RM], CHARINDEX('.', s.[Código RM]) - 1), 6) AS [Código RM Formatado],
    sugestão
FROM 
    {{ ref('gld_cubo33_2024') }} AS e
LEFT JOIN 
    {{ ref('slv_parceiros_novo_perfil') }} AS s
ON 
    e.[CÓDIGO DO CLIENTE] COLLATE SQL_Latin1_General_CP1_CI_AS = s.[Código RM] COLLATE SQL_Latin1_General_CP1_CI_AS
