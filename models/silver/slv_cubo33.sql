with base as (
    select *
    from {{ ref('brz_cubo33') }}
),

filtrados as (
    select *
    from base
    where [CÓDIGO DO CLIENTE] = '078646'
      and [CÓDIGO PRODUTO] = '25.46117.00117'
),

somas as (
    select
        sum(try_cast([FATURAMENTO SEM DESCONTO] as numeric(18, 2))) as [FATURAMENTO SEM DESCONTO],
        sum(try_cast([RECEITA] as numeric(18, 2))) as [RECEITA],
        sum(try_cast([VALOR TOTAL DESCONTO] as numeric(18, 2))) as [VALOR TOTAL DESCONTO]
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
    select
        o.[ASSISTENTE],
        o.[CÓDIGO DO CLIENTE],
        o.[FAT. P/ ESCOLA OU LIVRARIA],
        o.[CÓD. RM FATURAMENTO],
        o.[NOME DO CLIENTE],
        o.[CIDADE],
        o.[UF],
        o.[SOLICITAÇÃO CLIENTE],
        o.[CÓDIGO PRODUTO],
        o.[DESCRIÇÃO],
        o.[COLEÇÃO],
        o.[VOLUME],
        o.[ANO],
        o.[CATEGORIA],
        o.[ALUNO/PROFESSOR],
        o.[PROVA],
        o.[PERNONALIZAÇÃO],
        o.[TIPO DE PEDIDO],
        o.[MOTIVO DEVOLUCAO],
        o.[2º SEMESTRE],
        o.[ANO DE UTILIZAÇÃO],
        o.[CONDIÇÃO DE PGTO],
        o.[TOTAL HISTÓRICO],
        o.[TOTAL ATUAL],
        o.[ID DO MOVIMENTO],
        o.[N DO MOVIMENTO],
        o.[STATUS],
        o.[CODTMV],
        o.[VALOR TABELA],
        try_cast(o.[FATURAMENTO SEM DESCONTO] as numeric(18,2)) as [FATURAMENTO SEM DESCONTO],
        try_cast(o.[RECEITA] as numeric(18,2)) as [RECEITA],
        o.[RECEITA2],
        o.[% DESC GERAL],
        try_cast(o.[VALOR TOTAL DESCONTO] as numeric(18,2)) as [VALOR TOTAL DESCONTO],
        o.[N/NR E OR],
        o.[QUANTIDADE PEDIDO],
        o.[DATA DA CONFIRMAÇÃO DO CLIENTE],
        o.[DATA DE CRIAÇÃO],
        o.[DATA DE APROVAÇÃO],
        o.[MÊS],
        o.[CRIADO POR],
        o.[APROVAÇÃO],
        o.[CODCOLIGADA],
        o.[DATA DE APLICAÇÃO],
        o.[DISPONIBILIZAÇÃO],
        o.[DATA BASE VENCIMENTO],
        o.[VALORFRETE],
        o.[TIPO FRETE],
        o.[UNIDADE ATENDIMENTO],
        o.[REDE DE ENSINO],
        o.[ID FAT],
        o.[IDMOV],
        o.[CODFILIAL],
        o.[NUMERO DA NOTA FISCAL],
        o.[DESCONTOITEM],
        o.[DESCMOVRAT],
        o.[CODTMVULTFAT],
        o.[IDMOVULTFAT],
        o.[VALORLIQUIDOULTFAT],
        o.[NUMERO DA NOTA FISCAL DA REMESSA],
        o.[PEDIDO SYDLE],
        o.[PERCENTUAL DESCONTO ITEM],
        o.[IDENTIFICADOR DA REMESSA],
        o.[AGRUPAMENTO_CLIENTE],
        o.[NOME_AGRUPAMENTO],
        o.[CARGA],
        'NORMAL' as TIPO_REGISTRO
    from base o
    where not (
        o.[CÓDIGO DO CLIENTE] = '078646'
        and o.[CÓDIGO PRODUTO] = '25.46117.00117'
    )
)

select * from outras_linhas
union all
select * from linha_ajustada;