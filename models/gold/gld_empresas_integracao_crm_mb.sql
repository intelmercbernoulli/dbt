with empresas as (
    select
        [accountid],
        [Nome Fantasia],
        [Razão Social],
        [Rede de Ensino],
        [Tipo de Relação],
        [CNPJ],
        [Estado],
        [Cidade],
        [Bairro],
        [CEP],
        [Logradouro],
        [Número],
        [Complemento],
        [Telefone Principal],
        [Código INEP],
        [Código Meu Bernoulli],
        [Código RM],
        [Gerência Pedagógica Atual],
        [Consultor Pedagógico Atual],
        [Consultor Bilíngue Atual],
        [Assistente de Tecnologia Educacional],
        [Início do Contrato]
    from {{ ref('slv_empresas_crm') }}
),

alunos as (
    select *
    from {{ ref('slv_aluno_potencial_integracao_crm_mb') }}
)

select
    e.*,
    -- Aqui aplicamos COALESCE apenas nas colunas de interesse da tabela alunos
    COALESCE(a.[EFAI 1º ano], 'Não') as [EFAI 1º ano],
    COALESCE(a.[EFAI 2º ano], 'Não') as [EFAI 2º ano],
    COALESCE(a.[EFAI 3º ano], 'Não') as [EFAI 3º ano],
    COALESCE(a.[EFAI 4º ano], 'Não') as [EFAI 4º ano],
    COALESCE(a.[EFAI 5º ano], 'Não') as [EFAI 5º ano],
    COALESCE(a.[EFAF 6º ano], 'Não') as [EFAF 6º ano],
    COALESCE(a.[EFAF 7º ano], 'Não') as [EFAF 7º ano],
    COALESCE(a.[EFAF 8º ano], 'Não') as [EFAF 8º ano],
    COALESCE(a.[EFAF 9º ano], 'Não') as [EFAF 9º ano],
    COALESCE(a.[EI 2 anos], 'Não') as [EI 2 anos],
    COALESCE(a.[EI 3 anos], 'Não') as [EI 3 anos],
    COALESCE(a.[EI 4 anos], 'Não') as [EI 4 anos],
    COALESCE(a.[EI 5 anos], 'Não') as [EI 5 anos],
    COALESCE(a.[EM 1ª série], 'Não') as [EM 1ª série],
    COALESCE(a.[EM 2ª série], 'Não') as [EM 2ª série],
    COALESCE(a.[EM 3ª série], 'Não') as [EM 3ª série],
    COALESCE(a.[Pré-vestibular anual], 'Não') as [Pré-vestibular anual],
    COALESCE(a.[Pré-vestibular semestral], 'Não') as [Pré-vestibular semestral]

from empresas e
left join alunos a
    on e.accountid = a.id_account
