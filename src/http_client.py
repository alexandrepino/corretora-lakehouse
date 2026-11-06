import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def criar_sessao(tentativas: int = 5, backoff: float = 2.0) -> requests.Session:
    '''Sessão HTTP com retry exponencial para erros transitórios e rate limit.'''
    retry = Retry(
        total=tentativas,
        backoff_factor=backoff,               # esperas crescentes: 2s, 4s, 8s, 16s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET', 'HEAD'],
        respect_retry_after_header=True,      # obedece o Retry-After enviado pela API
        raise_on_status=False,                # devolve a última resposta para o log
    )
    sessao = requests.Session()
    sessao.mount('https://', HTTPAdapter(max_retries=retry))
    sessao.headers.update({'User-Agent': 'corretora-lakehouse/1.0'})
    return sessao