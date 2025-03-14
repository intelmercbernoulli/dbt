WITH Parceiros AS (
    SELECT *
    FROM {{ ref('slv_parceiros_novo_perfil') }}
    WHERE [Tipo de Relação] IN ('Parceiro')
),
CodigosPadronizados AS (
    SELECT 
        e.*, 
        RIGHT(REPLICATE('0', 6) + 
             CASE 
                 WHEN CHARINDEX('.', e.[Código RM]) > 0 
                 THEN LEFT(e.[Código RM], CHARINDEX('.', e.[Código RM]) - 1) 
                 ELSE e.[Código RM] 
             END, 6) COLLATE SQL_Latin1_General_CP1_CI_AS AS CodigoPadronizado
    FROM Parceiros e
),
ResultSet AS (
    SELECT 
        e.*, 
        s.[CÓDIGO DO CLIENTE], 
        s.alunos,
        ROW_NUMBER() OVER (PARTITION BY e.[accountid] ORDER BY e.[accountid]) AS rn
    FROM CodigosPadronizados AS e
    LEFT JOIN {{ ref('gld_cubo33_2024_alunado') }} AS s
    ON e.CodigoPadronizado = 
       RIGHT(REPLICATE('0', 6) + s.[CÓDIGO DO CLIENTE], 6) COLLATE SQL_Latin1_General_CP1_CI_AS
)
SELECT *
FROM ResultSet
WHERE rn = 1