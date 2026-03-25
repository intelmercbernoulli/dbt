WITH base AS (

SELECT
    e.[uid],
    e.[order_group],
    e.[order_number],
    e.[status],
    e.[subtotal_price],
    e.[discount_amount],
    e.[interest_amount],
    e.[tax_amount],
    e.[order_total],
    CAST(e.[date_created] AS DATETIMEOFFSET) AS date_created,
    CAST(e.[date_updated] AS DATETIMEOFFSET) AS date_updated,
    e.[type],
    e.[legal_entity_type],
    e.[organization],
    e.[charge_type],
    e.[is_reverted],
    e.[external_id],
    e.[customer.document],
    e.[customer.document_type],
    e.[customer.external_id],

    -- expansão raizes
    r.Marca              AS r_Marca,
    r.idPedido           AS r_idPedido,
    r.StatusPedido       AS r_StatusPedido,
    TRY_CONVERT(DATETIME, r.DataPedido, 103) AS r_DataPedido,
    TRY_CONVERT(DATETIME, r.DataStatusAtual, 103) AS r_DataStatusAtual,
    r.Escola             AS r_Escola,
    r.RazaoSocialEscola  AS r_RazaoSocialEscola,
    r.PedidoCliente,
    r.PedidoCliente2,
    r.GrupoFaturamento,
    r.Cidade,
    r.Responsavel,
    r.CpfCnpj,
    r.Email,
    r.QtdProdutos,
    r.PesoTotal,
    r.ValorFrete,
    r.ValorTotal,
    r.Nfs,
    r.CodigoAcesso,
    r.TipoPedido,
    ISNULL(r.Transportadora, '-') AS r_Transportadora,
    r.PrazoFrete,
    r.Volume,
    r.NfRemessaCliente,
    r.FreteAgrupadoCom,
    r.DataLiberacao,
    r.UF,
    r.Cep,
    r.AnoProjeto,
    r.Observacao,
    r.NfRemessaDowload,
    r.CnpjEscola,
    r.ClienteClienteCodigo,
    r.EscolaClienteCodigo,
    r.Aglomerado,
    r.AglomeradoStatus,
    r.PrevisaoEntregaReal,

    -- metas
    m.[Código RM]        AS m_CodigoRM,
    m.[CNPJ ESCOLA]      AS m_CnpjEscola,
    m.[ESCOLA]           AS m_Escola,
    m.[Código PB]        AS m_CodigoPB,
    m.[Nome Escola]      AS m_NomeEscola,
    m.[Data Início das Aulas] AS m_DataInicioAulas,
    m.[Data de Corte]    AS m_DataCorte,

    -- Status da Entrega
    ISNULL(r.AglomeradoStatus, r.StatusPedido) AS Status_Entrega,

    -- Integrado
    CASE 
        WHEN r.PedidoCliente2 IS NULL OR r.PedidoCliente2 = '' THEN 'Não'
        ELSE 'Sim'
    END AS Integrado,

    -- Prazo
    CASE 
        WHEN TRY_CONVERT(DATETIME, r.DataPedido, 103) > m.[Data de Corte]
            THEN DATEADD(DAY, 20, TRY_CONVERT(DATETIME, r.DataPedido, 103))
        WHEN TRY_CONVERT(DATETIME, r.DataPedido, 103) <= m.[Data de Corte]
            THEN m.[Data Início das Aulas]
        ELSE NULL
    END AS prazo_ce01,

    -- Dias corridos
    CASE 
        WHEN r.StatusPedido IN (
            'Entrega realizada',
            'Entrega - Bernoulli',
            'Entrega - com reposição'
        )
        THEN DATEDIFF(
            DAY,
            TRY_CONVERT(DATETIME, r.DataPedido, 103),
            TRY_CONVERT(DATETIME, r.DataStatusAtual, 103)
        )
        ELSE DATEDIFF(
            DAY,
            TRY_CONVERT(DATETIME, r.DataPedido, 103),
            GETDATE()
        )
    END AS DiasCorridos,

    -- data_aglomerado
    CASE 
        WHEN r.Aglomerado IS NOT NULL 
            THEN TRY_CONVERT(DATETIME, r_ag.DataPedido, 103)

        WHEN r.Aglomerado IS NULL 
             AND r.TipoPedido IN ('Remessa Aglomerado B2C', 'B2C - Escola')
            THEN TRY_CONVERT(DATETIME, r.DataPedido, 103)

        ELSE NULL
    END AS data_aglomerado,

    -- NOVA COLUNA
    CASE 
        WHEN 
            CASE 
                WHEN r.Aglomerado IS NOT NULL 
                    THEN TRY_CONVERT(DATETIME, r_ag.DataPedido, 103)
                WHEN r.Aglomerado IS NULL 
                     AND r.TipoPedido IN ('Remessa Aglomerado B2C', 'B2C - Escola')
                    THEN TRY_CONVERT(DATETIME, r.DataPedido, 103)
                ELSE NULL
            END IS NOT NULL
        THEN DATEADD(
            DAY, 
            4, 
            CASE 
                WHEN r.Aglomerado IS NOT NULL 
                    THEN TRY_CONVERT(DATETIME, r_ag.DataPedido, 103)
                WHEN r.Aglomerado IS NULL 
                     AND r.TipoPedido IN ('Remessa Aglomerado B2C', 'B2C - Escola')
                    THEN TRY_CONVERT(DATETIME, r.DataPedido, 103)
                ELSE NULL
            END
        )
        ELSE NULL
    END AS data_expedicao_aglomerada

FROM {{ source('intel_merc', 'brz_pedidos_eskolare') }} e

LEFT JOIN {{ source('intel_merc', 'brz_pedidos_raizes') }} r
    ON e.order_number = r.PedidoCliente2

LEFT JOIN {{ source('intel_merc', 'brz_pedidos_raizes') }} r_ag
    ON r.Aglomerado = r_ag.idPedido

LEFT JOIN {{ source('intel_merc', 'brz_datas_escolas_eskolare') }} m
    ON r.CnpjEscola = m.[CNPJ ESCOLA]

)

SELECT * FROM base