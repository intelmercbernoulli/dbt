with base as (
    select *
    from {{ source('dm_ope', 'BI_VENDACOLECAO') }}
),

filtrados as (
    select *
    from base
    where [CÓDIGO DO CLIENTE] = '078646'
      and [CÓDIGO PRODUTO] = '25.46117.00117'
),

somas as (
    select
        sum(cast([FATURAMENTO SEM DESCONTO] as numeric(18, 2))) as [FATURAMENTO SEM DESCONTO],
        sum(cast([RECEITA] as numeric(18, 2))) as [RECEITA],
        sum(cast([VALOR TOTAL DESCONTO] as numeric(18, 2))) as [VALOR TOTAL DESCONTO]
    from filtrados
),

ultima_linha as (
    select top 1 *
    from filtrados
    order by [DATA DE CRIAÇÃO] desc
),

linha_ajustada as (
    select
        u.[ASSISTENTE],
        u.[CÓDIGO DO CLIENTE],
        u.[FAT. P/ ESCOLA OU LIVRARIA],
        u.[CÓD. RM FATURAMENTO],
        u.[NOME DO CLIENTE],
        u.[CIDADE],
        u.[UF],
        u.[SOLICITAÇÃO CLIENTE],
        u.[CÓDIGO PRODUTO],
        u.[DESCRIÇÃO],
        u.[COLEÇÃO],
        u.[VOLUME],
        u.[ANO],
        u.[CATEGORIA],
        u.[ALUNO/PROFESSOR],
        u.[PROVA],
        u.[PERNONALIZAÇÃO],
        u.[TIPO DE PEDIDO],
        u.[MOTIVO DEVOLUCAO],
        u.[2º SEMESTRE],
        u.[ANO DE UTILIZAÇÃO],
        u.[CONDIÇÃO DE PGTO],
        u.[TOTAL HISTÓRICO],
        u.[TOTAL ATUAL],
        u.[ID DO MOVIMENTO],
        u.[N DO MOVIMENTO],
        u.[STATUS],
        u.[CODTMV],
        u.[VALOR TABELA],
        s.[FATURAMENTO SEM DESCONTO],
        s.[RECEITA],
        u.[RECEITA2],
        u.[% DESC GERAL],
        s.[VALOR TOTAL DESCONTO],
        u.[N/NR E OR],
        u.[QUANTIDADE PEDIDO],
        u.[DATA DA CONFIRMAÇÃO DO CLIENTE],
        u.[DATA DE CRIAÇÃO],
        u.[DATA DE APROVAÇÃO],
        u.[MÊS],
        u.[CRIADO POR],
        u.[APROVAÇÃO],
        u.[CODCOLIGADA],
        u.[DATA DE APLICAÇÃO],
        u.[DISPONIBILIZAÇÃO],
        u.[DATA BASE VENCIMENTO],
        u.[VALORFRETE],
        u.[TIPO FRETE],
        u.[UNIDADE ATENDIMENTO],
        u.[REDE DE ENSINO],
        u.[ID FAT],
        u.[IDMOV],
        u.[CODFILIAL],
        u.[NUMERO DA NOTA FISCAL],
        u.[DESCONTOITEM],
        u.[DESCMOVRAT],
        u.[CODTMVULTFAT],
        u.[IDMOVULTFAT],
        u.[VALORLIQUIDOULTFAT],
        u.[NUMERO DA NOTA FISCAL DA REMESSA],
        u.[PEDIDO SYDLE],
        u.[PERCENTUAL DESCONTO ITEM],
        u.[IDENTIFICADOR DA REMESSA],
        u.[AGRUPAMENTO_CLIENTE],
        u.[NOME_AGRUPAMENTO],
        u.[CARGA],
        'AGRUPADO' as TIPO_REGISTRO
    from ultima_linha u
    cross join somas s
),

outras_linhas as (
    select *,
        'NORMAL' as TIPO_REGISTRO
    from base
    where not (
        [CÓDIGO DO CLIENTE] = '078646'
        and [CÓDIGO PRODUTO] = '25.46117.00117'
    )
)

select * from outras_linhas
union all
select * from linha_ajustada