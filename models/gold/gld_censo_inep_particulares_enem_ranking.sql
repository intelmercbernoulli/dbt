with base as (

    select
        NU_ANO_CENSO,
        CO_ENTIDADE,
        NO_ENTIDADE,
        NO_MUNICIPIO,
        CO_MUNICIPIO,
        NO_REGIAO,
        SG_UF,

        QT_MAT_BAS,
        QT_MAT_INF,
        QT_MAT_FUND_AI,
        QT_MAT_FUND_AF,
        QT_MAT_MED,

        alunos,

        nota_enem,
        NU_NOTA_CN,
        NU_NOTA_CH,
        NU_NOTA_LC,
        NU_NOTA_MT,
        NU_NOTA_REDACAO

    from {{ ref('slv_censo_inep_2024_particulares_enem') }}
    where nota_enem is not null

),

-- =========================
-- Ranking geral (todas)
-- =========================
rank_geral as (

    select
        CO_ENTIDADE,
        rank() over (
            order by nota_enem desc
        ) as ranking_geral_nota_enem
    from base

),

-- =========================
-- Ranking geral (>= 100 alunos)
-- =========================
rank_geral_100 as (

    select
        CO_ENTIDADE,
        rank() over (
            order by nota_enem desc
        ) as ranking_geral_nota_enem_alunos_100
    from base
    where alunos >= 100

),

-- =========================
-- Ranking por UF (todas)
-- =========================
rank_uf as (

    select
        CO_ENTIDADE,
        rank() over (
            partition by SG_UF
            order by nota_enem desc
        ) as ranking_uf_nota_enem
    from base

),

-- =========================
-- Ranking por UF (>= 100 alunos)
-- =========================
rank_uf_100 as (

    select
        CO_ENTIDADE,
        rank() over (
            partition by SG_UF
            order by nota_enem desc
        ) as ranking_uf_nota_enem_alunos_100
    from base
    where alunos >= 100

),

-- =========================
-- Ranking Município + UF (todas)
-- =========================
rank_municipio_uf as (

    select
        CO_ENTIDADE,
        rank() over (
            partition by SG_UF, NO_MUNICIPIO
            order by nota_enem desc
        ) as ranking_municipio_uf_nota_enem
    from base

),

-- =========================
-- Ranking Município + UF (>= 100 alunos)
-- =========================
rank_municipio_uf_100 as (

    select
        CO_ENTIDADE,
        rank() over (
            partition by SG_UF, NO_MUNICIPIO
            order by nota_enem desc
        ) as ranking_municipio_uf_nota_enem_alunos_100
    from base
    where alunos >= 100

)

-- =========================
-- Final
-- =========================
select
    b.*,

    rg.ranking_geral_nota_enem,
    rg100.ranking_geral_nota_enem_alunos_100,

    ruf.ranking_uf_nota_enem,
    ruf100.ranking_uf_nota_enem_alunos_100,

    rmun.ranking_municipio_uf_nota_enem,
    rmun100.ranking_municipio_uf_nota_enem_alunos_100

from base b

left join rank_geral rg
    on b.CO_ENTIDADE = rg.CO_ENTIDADE

left join rank_geral_100 rg100
    on b.CO_ENTIDADE = rg100.CO_ENTIDADE

left join rank_uf ruf
    on b.CO_ENTIDADE = ruf.CO_ENTIDADE

left join rank_uf_100 ruf100
    on b.CO_ENTIDADE = ruf100.CO_ENTIDADE

left join rank_municipio_uf rmun
    on b.CO_ENTIDADE = rmun.CO_ENTIDADE

left join rank_municipio_uf_100 rmun100
    on b.CO_ENTIDADE = rmun100.CO_ENTIDADE