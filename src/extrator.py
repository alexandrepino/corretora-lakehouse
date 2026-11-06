import time
from datetime import datetime, timezone

from databricks.sdk.runtime import spark
from pyspark.sql.types import (BooleanType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

from src.config import TABELA_LOG
from src.raw_writer import gravar_arquivo, gravar_envelope

SCHEMA_LOG = StructType([
    StructField('id_coleta', StringType()),
    StructField('fonte', StringType()),
    StructField('entidade', StringType()),
    StructField('url', StringType()),
    StructField('status_code', IntegerType()),
    StructField('sucesso', BooleanType()),
    StructField('latencia_ms', LongType()),
    StructField('arquivo_raw', StringType()),
    StructField('erro', StringType()),
    StructField('executado_em_utc', TimestampType()),
])


def _log(fonte, entidade, url, inicio, status_code=None, sucesso=False,
         id_coleta=None, arquivo_raw=None, erro=None):
    return {
        'id_coleta': id_coleta,
        'fonte': fonte,
        'entidade': entidade,
        'url': url,
        'status_code': status_code,
        'sucesso': sucesso,
        'latencia_ms': int((time.monotonic() - inicio) * 1000),
        'arquivo_raw': arquivo_raw,
        'erro': erro[:500] if erro else None,
        'executado_em_utc': datetime.now(timezone.utc),
    }


def coletar_json(sessao, fonte, entidade, url, headers=None, params=None, timeout=30):
    '''Chama uma API JSON e grava o envelope na Raw. Nunca levanta exceção: devolve o log.'''
    inicio = time.monotonic()
    try:
        r = sessao.get(url, headers=headers, params=params, timeout=timeout)
        if r.status_code != 200:
            return _log(fonte, entidade, url, inicio, r.status_code, erro=r.text)
        arquivo, id_coleta = gravar_envelope(fonte, entidade, url, r.status_code, r.json(), params)
        return _log(fonte, entidade, url, inicio, r.status_code, True, id_coleta, arquivo)
    except Exception as e:
        return _log(fonte, entidade, url, inicio, erro=f'{type(e).__name__}: {e}')


def coletar_arquivo(sessao, fonte, entidade, url, extensao, timeout=120):
    '''Baixa um arquivo (CSV, ZIP...) e grava na Raw como veio.'''
    inicio = time.monotonic()
    try:
        r = sessao.get(url, timeout=timeout)
        if r.status_code != 200:
            return _log(fonte, entidade, url, inicio, r.status_code, erro=r.text)
        arquivo, id_coleta = gravar_arquivo(fonte, entidade, r.content, extensao)
        return _log(fonte, entidade, url, inicio, r.status_code, True, id_coleta, arquivo)
    except Exception as e:
        return _log(fonte, entidade, url, inicio, erro=f'{type(e).__name__}: {e}')


def finalizar(logs):
    '''Persiste o log e falha a task se qualquer coleta tiver falhado.'''
    if logs:
        spark.createDataFrame(logs, schema=SCHEMA_LOG).write.mode('append').saveAsTable(TABELA_LOG)
    falhas = [l for l in logs if not l['sucesso']]
    print(f'{len(logs) - len(falhas)}/{len(logs)} coletas com sucesso')
    if falhas:
        detalhes = '; '.join(f"{f['entidade']} ({f['status_code']}): {f['erro']}" for f in falhas)
        raise RuntimeError(f'{len(falhas)} coleta(s) falharam -> {detalhes}')