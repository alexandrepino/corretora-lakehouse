import json
import os
import uuid
from datetime import datetime, timezone

from src.config import RAW


def _novo_caminho(fonte, entidade, extensao):
    agora = datetime.now(timezone.utc)
    id_coleta = str(uuid.uuid4())
    pasta = f'{RAW}/{fonte}/{entidade}/dt_coleta={agora:%Y-%m-%d}'
    os.makedirs(pasta, exist_ok=True)
    arquivo = f'{pasta}/{entidade}_{agora:%Y%m%dT%H%M%S}_{id_coleta[:8]}.{extensao}'
    return arquivo, id_coleta, agora


def gravar_envelope(fonte, entidade, url, status_code, payload, params=None):
    '''Grava a resposta de uma API com metadados de coleta (padrão envelope).'''
    arquivo, id_coleta, agora = _novo_caminho(fonte, entidade, 'json')
    envelope = {
        'id_coleta': id_coleta,
        'fonte': fonte,
        'entidade': entidade,
        'url': url,
        'params': json.dumps(params or {}, ensure_ascii=False),
        'status_code': status_code,
        'coletado_em_utc': agora.isoformat(),
        'payload': json.dumps(payload, ensure_ascii=False),
    }
    with open(arquivo, 'w', encoding='utf-8') as f:
        f.write(json.dumps(envelope, ensure_ascii=False))
    return arquivo, id_coleta


def gravar_arquivo(fonte, entidade, conteudo: bytes, extensao):
    '''Grava um arquivo de origem exatamente como veio (ex.: CSV do Tesouro).'''
    arquivo, id_coleta, _ = _novo_caminho(fonte, entidade, extensao)
    with open(arquivo, 'wb') as f:
        f.write(conteudo)
    return arquivo, id_coleta


def gravar_eventos(entidade, eventos):
    '''Grava eventos de sistemas internos simulados em JSON Lines.'''
    arquivo, _, _ = _novo_caminho('sintetico', entidade, 'json')
    with open(arquivo, 'w', encoding='utf-8') as f:
        f.write('\n'.join(json.dumps(e, ensure_ascii=False) for e in eventos))
    return arquivo