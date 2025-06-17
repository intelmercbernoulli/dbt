--WIP
WITH crm AS(
    SELECT [accountid]
      ,[Nome Fantasia]
      ,[Quantidade Total de Alunos]
      ,[Rede de Ensino]
      ,[Estado]
      ,[Cidade]
      ,[Tipo de Relação]
      ,[Área de Atuação]
      ,[Cod RM Ajustado]
      ,[Cod RM 1 Ajustado]
      ,[Código INEP]
      ,[Curva ABCD]
      ,[Perfil da escola]
  FROM {{ ref ('slv_empresas_crm') }}
  WHERE [Tipo de Relação] IN (
    'BSE - Em Processo de Rescisão', 'BSE - Novo Parceiro', 'BSE - Ex-Cliente', 'BSE - Parceiro'
    )
),

rm AS (
    SELECT
    [CÓDIGO DO CLIENTE]
      ,[FATURAMENTO SEM DESCONTO]
      ,[RECEITA]
      ,[ALUNOS]
    FROM {{ ref ('gld_2025_escola_faturamento_alunos') }}
)

SELECT
    c.[accountid]
    ,c.[Nome Fantasia]
    ,c.[Quantidade Total de Alunos]
    ,c.[Rede de Ensino]
    ,c.[Estado]
    ,c.[Cidade]
    ,c.[Tipo de Relação]
    ,c.[Área de Atuação]
    ,c.[Cod RM Ajustado]
    ,c.[Cod RM 1 Ajustado]
    ,c.[Código INEP]
    ,c.[Curva ABCD]
    ,c.[Perfil da escola]
    ,r.[FATURAMENTO SEM DESCONTO]
    ,r.[RECEITA]
    ,r.[ALUNOS]   
FROM crm c 
LEFT JOIN rm r
    ON c.[Cod RM Ajustado] COLLATE SQL_Latin1_General_CP1_CI_AI = r.[CÓDIGO DO CLIENTE] COLLATE SQL_Latin1_General_CP1_CI_AI
    -- Ver código rm 1 e ver os casos das duplicadas
