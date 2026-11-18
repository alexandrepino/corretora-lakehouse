from pyspark.sql import functions as F

from src.config import CODIGOS_CRIPTO
from src.schemas import (SCHEMA_AWESOME_CATALOGO, SCHEMA_AWESOME_DIARIO, SCHEMA_AWESOME_ULTIMA,
                         SCHEMA_BCB, SCHEMA_BRAPI_CATALOGO, SCHEMA_BRAPI_COTACAO,
                         SCHEMA_BRAPI_HISTORICO)

FUSO_BR = 'America/Sao_Paulo'
ATRIBUTOS_CLIENTE = ['nome', 'cpf', 'email', 'data_nascimento', 'cidade', 'uf',
                     'perfil_investidor', 'faixa_renda']


def try_cast(expr_sql, tipo):
    '''Conversão tolerante: devolve NULL em vez de erro quando o valor é inválido.'''
    return F.expr(f'try_cast({expr_sql} AS {tipo})')


def data_texto(expr_sql, formato):
    '''Converte texto em data no formato informado; texto inválido vira NULL.'''
    return F.expr(f"to_date(try_to_timestamp({expr_sql}, '{formato}'))")


def decimal_br(expr_sql, tipo):
    '''Converte número com vírgula decimal (7,05) para DECIMAL.'''
    return F.expr(f"try_cast(replace(trim({expr_sql}), ',', '.') AS {tipo})")


def epoch_para_timestamp(expr_sql):
    '''Aceita epoch em segundos ou em milissegundos.'''
    n = f'try_cast({expr_sql} AS BIGINT)'
    return F.expr(f'timestamp_seconds(CASE WHEN {n} > 100000000000 THEN {n} DIV 1000 ELSE {n} END)')


def data_brasilia(coluna_utc):
    '''Data de negócio: o dia no horário de Brasília.'''
    return F.to_date(F.from_utc_timestamp(coluna_utc, FUSO_BR))


def base_envelope(df):
    '''Colunas comuns a todas as tabelas Bronze de API.'''
    return df.select(
        F.col('id_coleta').alias('_id_coleta'),
        '_arquivo_origem',
        'url',
        try_cast('coletado_em_utc', 'TIMESTAMP').alias('coletado_em_utc'),
        'payload',
    )

def cotacoes_awesome(df):
    return (base_envelope(df)
        .select('*', F.explode(F.from_json('payload', SCHEMA_AWESOME_ULTIMA)).alias('par', 'c'))
        .select(
            F.col('c.code').alias('ativo'),
            F.lit('awesomeapi').alias('fonte'),
            epoch_para_timestamp('c.`timestamp`').alias('cotado_em_utc'),
            F.col('c.codein').alias('moeda_cotacao'),
            try_cast('c.bid', 'DECIMAL(20,8)').alias('preco_compra'),
            try_cast('c.ask', 'DECIMAL(20,8)').alias('preco_venda'),
            try_cast('c.high', 'DECIMAL(20,8)').alias('preco_maximo_dia'),
            try_cast('c.low', 'DECIMAL(20,8)').alias('preco_minimo_dia'),
            try_cast('c.pctChange', 'DECIMAL(12,4)').alias('variacao_pct_dia'),
            'coletado_em_utc', '_id_coleta', '_arquivo_origem'))


def cotacoes_brapi(df):
    return (base_envelope(df)
        .select('*', F.explode(F.from_json('payload', SCHEMA_BRAPI_COTACAO)['results']).alias('r'))
        .select(
            F.col('r.symbol').alias('ativo'),
            F.lit('brapi').alias('fonte'),
            F.coalesce(try_cast('r.regularMarketTime', 'TIMESTAMP'),
                       epoch_para_timestamp('r.regularMarketTime')).alias('cotado_em_utc'),
            F.coalesce(F.col('r.currency'), F.lit('BRL')).alias('moeda_cotacao'),
            try_cast('r.regularMarketPrice', 'DECIMAL(20,8)').alias('preco_compra'),
            try_cast('r.regularMarketPrice', 'DECIMAL(20,8)').alias('preco_venda'),
            try_cast('r.regularMarketDayHigh', 'DECIMAL(20,8)').alias('preco_maximo_dia'),
            try_cast('r.regularMarketDayLow', 'DECIMAL(20,8)').alias('preco_minimo_dia'),
            try_cast('r.regularMarketChangePercent', 'DECIMAL(12,4)').alias('variacao_pct_dia'),
            'coletado_em_utc', '_id_coleta', '_arquivo_origem'))
    

def precos_awesome(df):
    return (base_envelope(df)
        .withColumn('ativo', F.expr("nullif(regexp_extract(url, '/daily/([A-Z0-9]+)-', 1), '')"))
        .select('*', F.explode(F.from_json('payload', SCHEMA_AWESOME_DIARIO)).alias('p'))
        .withColumn('cotado_em_utc', epoch_para_timestamp('p.`timestamp`'))
        .select(
            'ativo',
            data_brasilia('cotado_em_utc').alias('data_pregao'),
            F.lit('awesomeapi').alias('fonte'),
            F.lit(None).cast('decimal(20,8)').alias('preco_abertura'),
            try_cast('p.high', 'DECIMAL(20,8)').alias('preco_maximo'),
            try_cast('p.low', 'DECIMAL(20,8)').alias('preco_minimo'),
            try_cast('p.bid', 'DECIMAL(20,8)').alias('preco_fechamento'),
            F.lit(None).cast('bigint').alias('volume'),
            'cotado_em_utc', 'coletado_em_utc', '_id_coleta', '_arquivo_origem'))


def precos_brapi(df):
    return (base_envelope(df)
        .select('*', F.explode(F.from_json('payload', SCHEMA_BRAPI_HISTORICO)['results']).alias('r'))
        .select('*', F.explode('r.historicalDataPrice').alias('h'))
        .withColumn('cotado_em_utc', epoch_para_timestamp('h.`date`'))
        .select(
            F.col('r.symbol').alias('ativo'),
            data_brasilia('cotado_em_utc').alias('data_pregao'),
            F.lit('brapi').alias('fonte'),
            try_cast('h.open', 'DECIMAL(20,8)').alias('preco_abertura'),
            try_cast('h.high', 'DECIMAL(20,8)').alias('preco_maximo'),
            try_cast('h.low', 'DECIMAL(20,8)').alias('preco_minimo'),
            try_cast('h.close', 'DECIMAL(20,8)').alias('preco_fechamento'),
            try_cast('h.volume', 'BIGINT').alias('volume'),
            'cotado_em_utc', 'coletado_em_utc', '_id_coleta', '_arquivo_origem'))
    

def ativos_awesome(df):
    return (base_envelope(df)
        .select('*', F.explode(F.from_json('payload', SCHEMA_AWESOME_CATALOGO)).alias('par', 'descricao'))
        .filter(F.col('par').endswith('-BRL'))
        .withColumn('ativo', F.split('par', '-').getItem(0))
        .withColumn('classe', F.when(F.col('ativo').isin(*CODIGOS_CRIPTO), 'CRIPTO').otherwise('MOEDA'))
        .select(
            'ativo',
            F.split('descricao', '/').getItem(0).alias('nome'),
            'classe',
            F.lit(None).cast('string').alias('setor'),
            F.lit('BRL').alias('moeda_cotacao'),
            F.when(F.col('classe') == 'CRIPTO', 8).otherwise(4).alias('casas_decimais'),
            F.lit(True).alias('negociavel'),
            F.lit('awesomeapi').alias('fonte'),
            'coletado_em_utc', '_id_coleta', '_arquivo_origem'))


def ativos_brapi(df):
    base = base_envelope(df).withColumn('j', F.from_json('payload', SCHEMA_BRAPI_CATALOGO))
    comuns = ['coletado_em_utc', '_id_coleta', '_arquivo_origem']
    papeis = (base.select('*', F.explode('j.stocks').alias('a'))
        .select(
            F.col('a.stock').alias('ativo'),
            F.col('a.name').alias('nome'),
            F.when(F.col('a.type') == 'stock', 'ACAO')
             .when(F.col('a.type') == 'fund', 'FUNDO')
             .when(F.col('a.type') == 'bdr', 'BDR')
             .otherwise('OUTRO').alias('classe'),
            F.col('a.sector').alias('setor'),
            F.lit(True).alias('negociavel'),
            *comuns))
    indices = (base.select('*', F.explode('j.indexes').alias('i'))
        .select(
            F.col('i.stock').alias('ativo'),
            F.col('i.name').alias('nome'),
            F.lit('INDICE').alias('classe'),
            F.lit(None).cast('string').alias('setor'),
            F.lit(False).alias('negociavel'),
            *comuns))
    return (papeis.unionByName(indices)
        .withColumn('moeda_cotacao', F.lit('BRL'))
        .withColumn('casas_decimais', F.lit(2))
        .withColumn('fonte', F.lit('brapi')))
    

def indicadores_bcb(df, referencia):
    return (base_envelope(df)
        .withColumn('codigo_sgs', F.expr("try_cast(regexp_extract(url, 'sgs[.]([0-9]+)', 1) AS INT)"))
        .select('*', F.explode(F.from_json('payload', SCHEMA_BCB)).alias('p'))
        .select(
            'codigo_sgs',
            data_texto('p.data', 'dd/MM/yyyy').alias('data_referencia'),
            try_cast('p.valor', 'DECIMAL(20,8)').alias('valor'),
            'coletado_em_utc', '_id_coleta', '_arquivo_origem')
        .join(F.broadcast(referencia), 'codigo_sgs', 'left'))


def tesouro(df):
    titulo = F.trim(F.col('tipo_titulo'))
    return df.select(
        titulo.alias('tipo_titulo'),
        data_texto('data_vencimento', 'dd/MM/yyyy').alias('data_vencimento'),
        data_texto('data_base', 'dd/MM/yyyy').alias('data_base'),
        F.when(titulo.contains('Selic'), 'SELIC')
         .when(titulo.contains('IGPM'), 'IGPM')
         .when(titulo.contains('Prefixado'), 'PREFIXADO')
         .otherwise('IPCA').alias('indexador'),
        decimal_br('taxa_compra_manha', 'DECIMAL(10,4)').alias('taxa_compra'),
        decimal_br('taxa_venda_manha', 'DECIMAL(10,4)').alias('taxa_venda'),
        decimal_br('pu_compra_manha', 'DECIMAL(18,6)').alias('pu_compra'),
        decimal_br('pu_venda_manha', 'DECIMAL(18,6)').alias('pu_venda'),
        decimal_br('pu_base_manha', 'DECIMAL(18,6)').alias('pu_base'),
        F.col('taxa_compra_manha').alias('_taxa_compra_bruta'),
        F.col('_data_ingestao').alias('_ingerido_em'),
        '_arquivo_origem')

def clientes_eventos(df):
    return df.select(
        'id_cliente',
        F.upper(F.trim('tipo_evento')).alias('tipo_evento'),
        F.trim('nome').alias('nome'),
        F.regexp_replace('cpf', '[^0-9]', '').alias('cpf'),
        F.lower(F.trim('email')).alias('email'),
        data_texto('data_nascimento', 'yyyy-MM-dd').alias('data_nascimento'),
        F.trim('cidade').alias('cidade'),
        F.upper(F.trim('uf')).alias('uf'),
        F.upper(F.trim('perfil_investidor')).alias('perfil_investidor'),
        F.upper(F.trim('faixa_renda')).alias('faixa_renda'),
        try_cast('evento_em_utc', 'TIMESTAMP').alias('evento_em_utc'),
        '_arquivo_origem')


def consolidar_clientes(eventos):
    '''Uma linha por cliente: o último valor NÃO NULO de cada atributo prevalece.'''
    ultimo_valor = [
        F.max_by(c, F.when(F.col(c).isNotNull(), F.col('evento_em_utc'))).alias(c)
        for c in ATRIBUTOS_CLIENTE
    ]
    return eventos.groupBy('id_cliente').agg(
        *ultimo_valor,
        F.min(F.when(F.col('tipo_evento') == 'CADASTRO', F.col('evento_em_utc'))).alias('cadastrado_em_utc'),
        F.max('evento_em_utc').alias('ultimo_evento_em_utc'),
        F.max_by('_arquivo_origem', 'evento_em_utc').alias('_arquivo_origem'))


def operacoes(df):
    return df.select(
        'id_operacao',
        'id_cliente',
        F.upper(F.trim('ativo')).alias('ativo'),
        F.upper('classe_ativo').alias('classe_ativo'),
        F.upper(F.trim('lado')).alias('lado'),
        try_cast('quantidade', 'DECIMAL(28,8)').alias('quantidade'),
        try_cast('preco_mercado', 'DECIMAL(20,8)').alias('preco_mercado'),
        try_cast('preco_executado', 'DECIMAL(20,8)').alias('preco_executado'),
        try_cast('valor_financeiro', 'DECIMAL(18,2)').alias('valor_financeiro'),
        try_cast('taxa_corretagem', 'DECIMAL(18,2)').alias('taxa_corretagem'),
        try_cast('receita_spread', 'DECIMAL(18,2)').alias('receita_spread'),
        F.upper('canal').alias('canal'),
        F.upper('status').alias('status'),
        try_cast('executada_em_utc', 'TIMESTAMP').alias('executada_em_utc'),
        F.col('_data_ingestao').alias('_ingerido_em'),
        '_arquivo_origem',
    ).withColumn('data_operacao', data_brasilia('executada_em_utc'))


    

