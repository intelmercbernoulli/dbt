with base as (

    select
        NU_ANO_CENSO,
        CO_ENTIDADE,
        NO_ENTIDADE,
        NO_MUNICIPIO,
        NO_REGIAO,
        SG_UF,
        QT_MAT_BAS,
        QT_MAT_INF,
        QT_MAT_FUND_AI,
        QT_MAT_FUND_AF,
        QT_MAT_MED,
        nota_enem,
        alunos
    from {{ ref('slv_censo_inep_2024_particulares_enem') }}
    where nota_enem is not null

),

rankings as (

    select
        *,

        /* Ranking geral por nota ENEM */
        rank() over (
            order by nota_enem desc
        ) as ranking_geral_nota_enem,

        /* Ranking geral considerando apenas escolas com alunos > 100 */
        case
            when alunos >= 100 then
                rank() over (
                    partition by case when alunos >= 100 then 1 end
                    order by nota_enem desc
                )
        end as ranking_geral_nota_enem_alunos_100,

        /* Ranking por UF */
        rank() over (
            partition by SG_UF
            order by nota_enem desc
        ) as ranking_uf_nota_enem,

        /* Ranking por UF considerando apenas escolas com alunos > 100 */
        case
            when alunos >= 100 then
                rank() over (
                    partition by SG_UF, case when alunos >= 100 then 1 end
                    order by nota_enem desc
                )
        end as ranking_uf_nota_enem_alunos_100

    from base

)

select *
from rankings
