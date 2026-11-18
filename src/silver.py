from datetime import timedelta

from databricks.sdk.runtime import spark
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.config import CATALOGO
from src.transformacoes import ATRIBUTOS_CLIENTE

TABELA_CONTROLE = f'{CATALOGO}.ops.controle_silver'
TABELA_QUARENTENA = f'{CATALOGO}.silver.quarentena'


# ---------- 1. Leitura incremental ----------

def ler_watermark(fluxo):
    linha = (spark.table(TABELA_CONTROLE)
             .filter(F.col('fluxo') == fluxo)
             .select('ultimo_ingerido_em')
             .first())
    return linha[0] if linha else None


def ler_incremento(tabela_bronze, fluxo, sobreposicao_minutos):
    df = spark.table(tabela_bronze)
    watermark = ler_watermark(fluxo)
    if watermark is not None:
        corte = watermark - timedelta(minutes=sobreposicao_minutos)
        df = df.filter(F.col('_data_ingestao') > F.lit(corte))
    return df


# ---------- 2. Qualidade ----------

def separar_por_regras(df, regras):
    '''Devolve (válidos, inválidos). Inválidos ganham a lista das regras violadas.'''
    checagens = [
        F.when(~F.coalesce(F.expr(condicao), F.lit(False)), F.lit(nome))
        for nome, condicao in regras.items()
    ]
    df = df.withColumn('_violacoes', F.filter(F.array(*checagens), lambda x: x.isNotNull()))
    validos = df.filter(F.size('_violacoes') == 0).drop('_violacoes')
    invalidos = df.filter(F.size('_violacoes') > 0)
    return validos, invalidos


def gravar_quarentena(invalidos, tabela_destino, fluxo):
    colunas = [c for c in invalidos.columns if c != '_violacoes']
    registros = (invalidos
        .withColumn('registro', F.to_json(F.struct(*colunas)))
        .select(
            F.sha2(F.concat_ws('|', F.lit(tabela_destino), F.lit(fluxo), F.col('registro')), 256)
             .alias('id_registro'),
            F.lit(tabela_destino).alias('tabela_destino'),
            F.lit(fluxo).alias('fluxo'),
            F.col('_violacoes').alias('regras_violadas'),
            'registro',
            F.current_timestamp().alias('quarentenado_em'))
        .dropDuplicates(['id_registro']))
    (DeltaTable.forName(spark, TABELA_QUARENTENA).alias('t')
        .merge(registros.alias('s'), 't.id_registro = s.id_registro')
        .whenNotMatchedInsertAll()
        .execute())


# ---------- 3. Deduplicação e gravação ----------

def deduplicar(df, chaves, ordem):
    '''Mantém uma linha por chave: a mais recente segundo as colunas de ordem.'''
    janela = Window.partitionBy(*chaves).orderBy(*[F.col(c).desc_nulls_last() for c in ordem])
    return df.withColumn('_posicao', F.row_number().over(janela)).filter('_posicao = 1').drop('_posicao')


def upsert(df, tabela, chaves, ordem):
    '''MERGE idempotente: insere o novo e atualiza só quando a origem é mais recente.'''
    colunas = spark.table(tabela).columns
    origem = df.withColumn('_processado_em', F.current_timestamp()).select(*colunas)
    condicao = ' AND '.join(f't.{c} = s.{c}' for c in chaves)
    (DeltaTable.forName(spark, tabela).alias('t')
        .merge(origem.alias('s'), condicao)
        .whenMatchedUpdateAll(condition=f's.{ordem[0]} > t.{ordem[0]}')
        .whenNotMatchedInsertAll()
        .execute())


def merge_clientes(df, tabela, chaves, ordem):
    '''Atualização parcial: o último valor não nulo de cada atributo prevalece.'''
    origem = df.withColumn('_processado_em', F.current_timestamp())
    novos_valores = {c: F.coalesce(F.col(f's.{c}'), F.col(f't.{c}')) for c in ATRIBUTOS_CLIENTE}
    novos_valores.update({
        'cadastrado_em_utc': F.coalesce(F.col('t.cadastrado_em_utc'), F.col('s.cadastrado_em_utc')),
        'ultimo_evento_em_utc': F.col('s.ultimo_evento_em_utc'),
        '_arquivo_origem': F.col('s._arquivo_origem'),
        '_processado_em': F.col('s._processado_em'),
    })
    (DeltaTable.forName(spark, tabela).alias('t')
        .merge(origem.alias('s'), 't.id_cliente = s.id_cliente')
        .whenMatchedUpdate(condition='s.ultimo_evento_em_utc > t.ultimo_evento_em_utc', set=novos_valores)
        .whenNotMatchedInsertAll()
        .execute())


# ---------- 4. Controle ----------

def salvar_controle(fluxo, tabela_silver, watermark, lidos, validos, quarentena):
    dados = spark.createDataFrame(
        [(fluxo, tabela_silver, watermark, lidos, validos, quarentena)],
        'fluxo string, tabela_silver string, ultimo_ingerido_em timestamp, '
        'registros_lidos bigint, registros_validos bigint, registros_quarentena bigint',
    ).withColumn('atualizado_em', F.current_timestamp())
    (DeltaTable.forName(spark, TABELA_CONTROLE).alias('t')
        .merge(dados.alias('s'), 't.fluxo = s.fluxo')
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


# ---------- 5. Orquestração de um fluxo ----------

def processar_fluxo(fluxo, tabela_bronze, tabela_silver, transformar, regras, chaves, ordem,
                    consolidar=None, merge=None, sobreposicao_minutos=30):
    bronze = ler_incremento(tabela_bronze, fluxo, sobreposicao_minutos)
    novo_watermark = bronze.agg(F.max('_data_ingestao')).first()[0]
    if novo_watermark is None:
        print(f'[sem novidades] {fluxo}')
        return

    validos, invalidos = separar_por_regras(transformar(bronze), regras)
    consolidados = consolidar(validos) if consolidar else deduplicar(validos, chaves, ordem)
    lidos, n_validos, n_invalidos = bronze.count(), validos.count(), invalidos.count()

    (merge or upsert)(consolidados, tabela_silver, chaves, ordem)
    if n_invalidos:
        gravar_quarentena(invalidos, tabela_silver, fluxo)
    salvar_controle(fluxo, tabela_silver, novo_watermark, lidos, n_validos, n_invalidos)
    print(f'[ok] {fluxo}: {lidos} linhas da Bronze, {n_validos} registros válidos, '
          f'{n_invalidos} em quarentena')