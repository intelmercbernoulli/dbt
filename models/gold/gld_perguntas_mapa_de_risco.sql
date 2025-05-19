SELECT *, 
CASE 
    WHEN [Pergunta] = 'A escola deixou de utilizar material de algum segmento?' THEN 'Deixou de utilizar material?'
    WHEN [Pergunta] = 'A escola está aderente à plataforma digital?' THEN 'Aderente à plataforma digital?'
    WHEN [Pergunta] = 'A escola está satisfeita com o suporte pedagógico?' THEN 'Satisfeita com o suporte pedagógico?'
    WHEN [Pergunta] = 'A escola está utilizando o material didático corretamente?' THEN 'Utilizando material corretamente?'
    WHEN [Pergunta] = 'A escola tem contrato vencendo em 2025?' THEN 'Contrato vencendo em 2025?'
    WHEN [Pergunta] = 'A metodologia de ensino está alinhada com a escola?' THEN 'Metodologia alinhada?'
    WHEN [Pergunta] = 'As condições de desconto, prazo e pagamento estão adequadas?' THEN 'Condições de pagamento adequadas?'
    WHEN [Pergunta] = 'Os pedidos, entregas e distribuição de materiais estão ocorrendo conforme esperado?' THEN 'Pedidos e distribuição conforme esperado?'
    ELSE [Pergunta]
END AS [Pergunta Resumida]
FROM
{{ source ('intel_merc', 'brz_perguntas_mapa_de_risco') }}