'''Regras de qualidade da Silver: nome da regra -> condição que o registro válido satisfaz.'''

COTACOES = {
    'ativo_ausente': "ativo IS NOT NULL AND ativo <> ''",
    'horario_invalido': 'cotado_em_utc IS NOT NULL',
    'preco_nao_positivo': 'preco_compra > 0',
    'spread_negativo': 'preco_venda >= preco_compra',
    'cotacao_no_futuro': 'cotado_em_utc <= coletado_em_utc + INTERVAL 10 MINUTES',
}

PRECOS_DIARIOS = {
    'ativo_ausente': "ativo IS NOT NULL AND ativo <> ''",
    'data_invalida': 'data_pregao IS NOT NULL',
    'fechamento_invalido': 'preco_fechamento > 0',
    'faixa_invalida': 'preco_maximo IS NULL OR preco_minimo IS NULL OR preco_maximo >= preco_minimo',
}

ATIVOS = {
    'ativo_ausente': "ativo IS NOT NULL AND ativo <> ''",
    'classe_ausente': 'classe IS NOT NULL',
}

INDICADORES = {
    'codigo_invalido': 'codigo_sgs IS NOT NULL',
    'data_invalida': 'data_referencia IS NOT NULL',
    'valor_invalido': 'valor IS NOT NULL',
    'serie_sem_referencia': 'nome_serie IS NOT NULL',
}

TESOURO = {
    'titulo_ausente': 'tipo_titulo IS NOT NULL',
    'datas_invalidas': 'data_vencimento IS NOT NULL AND data_base IS NOT NULL',
    'pu_base_invalido': 'pu_base > 0',
    'taxa_compra_ilegivel': "_taxa_compra_bruta IS NULL OR trim(_taxa_compra_bruta) = '' OR taxa_compra IS NOT NULL",
}

CLIENTES = {
    'id_cliente_ausente': 'id_cliente IS NOT NULL',
    'tipo_evento_invalido': "tipo_evento IN ('CADASTRO', 'ATUALIZACAO')",
    'evento_sem_data': 'evento_em_utc IS NOT NULL',
    'cpf_invalido': "tipo_evento <> 'CADASTRO' OR length(cpf) = 11",
    'nome_ausente_no_cadastro': "tipo_evento <> 'CADASTRO' OR nome IS NOT NULL",
    'uf_invalida': 'uf IS NULL OR length(uf) = 2',
    'perfil_invalido': "perfil_investidor IS NULL OR perfil_investidor IN ('CONSERVADOR', 'MODERADO', 'ARROJADO')",
}

OPERACOES = {
    'id_operacao_ausente': 'id_operacao IS NOT NULL',
    'id_cliente_ausente': 'id_cliente IS NOT NULL',
    'lado_invalido': "lado IN ('COMPRA', 'VENDA')",
    'quantidade_invalida': 'quantidade > 0',
    'preco_invalido': 'preco_executado > 0',
    'valor_inconsistente': 'abs(valor_financeiro - quantidade * preco_executado) <= 0.05',
    'execucao_sem_data': 'executada_em_utc IS NOT NULL',
}