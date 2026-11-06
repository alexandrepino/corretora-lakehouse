CATALOGO = 'corretora'
RAW = f'/Volumes/{CATALOGO}/raw/landing'
CHECKPOINTS = f'/Volumes/{CATALOGO}/ops/checkpoints'
TABELA_LOG = f'{CATALOGO}.ops.log_ingestao'
SECRET_SCOPE = 'corretora'

# AwesomeAPI: moedas fiat e cripto cotadas em BRL
PARES_AWESOME = ['USD-BRL', 'EUR-BRL', 'GBP-BRL', 'BTC-BRL', 'ETH-BRL']

# brapi: ações, ETF, FII e índice da B3 (^BVSP é índice, não negociável)
TICKERS_BRAPI = ['PETR4', 'VALE3', 'ITUB4', 'MGLU3', 'BBAS3', 'WEGE3', 'BOVA11', 'HGLG11', '^BVSP']

# Banco Central: séries do SGS
SERIES_BCB = {
    'selic_meta': 432,
    'selic_diaria': 11,
    'cdi_diario': 12,
    'ipca_mensal': 433,
    'igpm_mensal': 189,
    'ptax_usd_venda': 1,
}

# Tesouro Direto: preços e taxas históricos (Tesouro Transparente).
# Se a URL mudar, copie a nova no dataset 'Taxas dos Títulos Ofertados pelo Tesouro Direto'.
URL_TESOURO = (
    'https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3'
    '/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv'
)

# Regras de negócio da corretora
SPREAD_CORRETORA = 0.005       # 0,5% sobre o preço de mercado
TAXA_CORRETAGEM_PCT = 0.001    # 0,1% do valor financeiro

# Fontes da Bronze: (tabela, pasta na Raw, formato, opções do leitor)
FONTES_BRONZE = [
    ('awesomeapi_cotacoes_atuais', 'awesomeapi/cotacoes_atuais', 'json', {}),
    ('awesomeapi_historico_diario', 'awesomeapi/historico_diario', 'json', {}),
    ('awesomeapi_catalogo_pares', 'awesomeapi/catalogo_pares', 'json', {}),
    ('brapi_cotacoes', 'brapi/cotacoes', 'json', {}),
    ('brapi_historico', 'brapi/historico', 'json', {}),
    ('brapi_catalogo_ativos', 'brapi/catalogo_ativos', 'json', {}),
    ('bcb_series', 'bcb/series', 'json', {}),
    ('tesouro_precos_taxas', 'tesouro/precos_taxas', 'csv', {'header': 'true', 'sep': ';'}),
    ('clientes', 'sintetico/clientes', 'json', {}),
    ('operacoes', 'sintetico/operacoes', 'json', {}),
]