WITH base AS (

    SELECT
        *,

        -- Status do Prazo
        CASE 
            WHEN prazo_ce01 IS NULL THEN '-'
            WHEN Status_Entrega IN (
                'Entrega realizada',
                'Entrega - Bernoulli',
                'Entrega - com reposição'            )
            THEN 
                CASE 
                    WHEN r_DataStatusAtual IS NULL THEN '-'
                    WHEN r_DataStatusAtual <= prazo_ce01 THEN 'Dentro do Prazo'
                    ELSE 'Fora do Prazo'
                END
            ELSE
                CASE 
                    WHEN GETDATE() <= prazo_ce01 THEN 'Dentro do Prazo'
                    ELSE 'Fora do Prazo'
                END
        END AS status_prazo_ce01,

        -- ordem (status)
        CASE Status_Entrega
            WHEN 'Pedido Realizado' THEN 0
            WHEN 'Aguardando definição de lote' THEN 1
            WHEN 'Aguardando gerar NF' THEN 2
            WHEN 'Pedido Finalizado' THEN 3
            WHEN 'Aguardando liberação para despacho' THEN 4
            WHEN 'Aguardando importação' THEN 5
            WHEN 'Operador logistico - Em validaçāo' THEN 6
            WHEN 'Operador logistico - Em fila para separação' THEN 7
            WHEN 'Operador logistico - Em picking/packing' THEN 8
            WHEN 'Aguardando cotação de frete' THEN 9
            WHEN 'Aguardando aprovação do frete' THEN 10
            WHEN 'Aguardando gerar NF Remessa' THEN 11
            WHEN 'Pronto para expedir' THEN 12
            WHEN 'Despachado' THEN 13
            WHEN 'Em trânsito' THEN 14
            WHEN 'Entrega realizada' THEN 15
            WHEN 'Entrega - Bernoulli' THEN 16
            WHEN 'Entrega - com reposição' THEN 17
            WHEN 'Devolução remetente' THEN 18
            WHEN 'Cancelado' THEN 19
            WHEN 'Cancelado (Raízes)' THEN 20
            WHEN 'Excluido' THEN 21
            ELSE -1
        END AS ordem,

        -- Dias atraso
        CASE 
            WHEN prazo_ce01 IS NULL THEN NULL

            WHEN (
                CASE 
                    WHEN Status_Entrega IN (
                        'Entrega realizada',
                        'Entrega - Bernoulli',
                        'Entrega - com reposição'
                    )
                    THEN 
                        CASE WHEN r_DataStatusAtual <= prazo_ce01 THEN 0 ELSE 1 END
                    ELSE 
                        CASE WHEN GETDATE() <= prazo_ce01 THEN 0 ELSE 1 END
                END
            ) = 1

            THEN 
                CASE 
                    WHEN Status_Entrega IN (
                        'Entrega realizada',
                        'Entrega - Bernoulli',
                        'Entrega - com reposição'
                    )
                    THEN DATEDIFF(DAY, prazo_ce01, r_DataStatusAtual)
                    ELSE DATEDIFF(DAY, prazo_ce01, GETDATE())
                END
        END AS DiasAtraso,

        -- Categoria
        CASE 
            WHEN Status_Entrega = 'Aguardando definição de lote' THEN 'Ag Lote'
            WHEN Status_Entrega = 'Aguardando gerar NF' THEN 'Ag NF'
            WHEN Status_Entrega = 'Aguardando gerar NF Remessa' THEN 'Ag NF Remessa'
            WHEN Status_Entrega = 'Aguardando Estoque' THEN 'Ag Estoque'
            WHEN Status_Entrega = 'Pronto para expedir' THEN 'Ag Expedir'
            WHEN Status_Entrega = 'Devolução remetente' THEN 'Devolução'

            WHEN Status_Entrega IN (
                'Aguardando liberação para despacho',
                'Aguardando importação'
            ) THEN 'Ag Despacho'

            WHEN Status_Entrega IN (
                'Operador logistico - Em validaçāo',
                'Operador logistico - Em validação',
                'Operador logistico - Em fila para separação',
                'Operador logistico - Em picking/packing'
            ) THEN 'Em Operação'

            WHEN Status_Entrega IN (
                'Aguardando cotação de frete',
                'Aguardando aprovação do frete'
            ) THEN 'Ag Frete'

            WHEN Status_Entrega IN ('Despachado','Em trânsito') THEN 'Em Logística'

            WHEN Status_Entrega IN (
                'Entrega realizada',
                'Entrega - com reposição',
                'Entrega - Bernoulli'
            ) THEN 'Entregue'

            WHEN Status_Entrega IN (
                'Cancelado',
                'Cancelado (Raízes)',
                'Excluido'
            ) THEN 'Cancelado'

            ELSE Status_Entrega
        END AS Categoria

    FROM {{ ref('slv_pedidos_base') }}

),

final AS (

    SELECT
        *,

        -- agora sim pode usar Categoria
        CASE 
            WHEN Categoria = 'Pedido Realizado' THEN 0
            WHEN Categoria = 'Ag Lote' THEN 1
            WHEN Categoria = 'AG NF' THEN 2
            WHEN Categoria = 'Pedido Finalizado' THEN 3
            WHEN Categoria = 'Ag Despacho' THEN 4
            WHEN Categoria = 'Em Operação' THEN 5
            WHEN Categoria = 'Ag Frete' THEN 6
            WHEN Categoria = 'Ag NF Remessa' THEN 7
            WHEN Categoria = 'Ag Expedir' THEN 8
            WHEN Categoria = 'Em Logística' THEN 9
            WHEN Categoria = 'Entregue' THEN 10
            WHEN Categoria = 'Devolução' THEN 11
            WHEN Categoria = 'Cancelado' THEN 12
            ELSE -1
        END AS OrdemCategoria

    FROM base

)

SELECT *
FROM final
WHERE r_RazaoSocialEscola NOT LIKE '%COLEGIO SANTANNA%'