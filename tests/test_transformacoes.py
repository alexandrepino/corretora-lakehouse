from datetime import datetime
from decimal import Decimal

from pyspark.sql import functions as F

from src.regras import TESOURO
from src.silver import separar_por_regras
from src.transformacoes import consolidar_clientes, epoch_para_timestamp, tesouro

COLUNAS_TESOURO = (
    'tipo_titulo string, data_vencimento string, data_base string, taxa_compra_manha string, '
    'taxa_venda_manha string, pu_compra_manha string, pu_venda_manha string, pu_base_manha string, '
    '_data_ingestao timestamp, _arquivo_origem string'
)


def linha_tesouro(taxa_compra='7,05'):
    return ('Tesouro IPCA+', '15/05/2045', '30/10/2026', taxa_compra, '7,17',
            '1234,56', '1220,10', '1225,00', datetime(2026, 11, 1), 'precos.csv')


def test_epoch_em_segundos_e_em_milissegundos_viram_o_mesmo_instante(spark):
    df = spark.createDataFrame([('1618315045',), ('1618315045000',)], 'ts string')
    resultado = [r[0] for r in df.select(
        F.date_format(epoch_para_timestamp('ts'), 'yyyy-MM-dd HH:mm:ss')).collect()]
    assert resultado == ['2021-04-13 11:57:25', '2021-04-13 11:57:25']


def test_tesouro_converte_decimal_brasileiro_datas_e_indexador(spark):
    linha = tesouro(spark.createDataFrame([linha_tesouro()], COLUNAS_TESOURO)).first()
    assert linha.indexador == 'IPCA'
    assert linha.taxa_compra == Decimal('7.05')
    assert linha.pu_compra == Decimal('1234.56')
    assert str(linha.data_vencimento) == '2045-05-15'


def test_taxa_ilegivel_vai_para_quarentena_em_vez_de_virar_nulo_silencioso(spark):
    df = tesouro(spark.createDataFrame([linha_tesouro(taxa_compra='abc')], COLUNAS_TESOURO))
    validos, invalidos = separar_por_regras(df, TESOURO)
    assert validos.count() == 0
    assert invalidos.first()._violacoes == ['taxa_compra_ilegivel']


def test_atualizacao_parcial_preserva_os_campos_nao_enviados(spark):
    eventos = spark.createDataFrame([
        ('c1', 'CADASTRO', 'Ana Souza', '12345678901', 'ana@email.com', None,
         'Goiânia', 'GO', 'MODERADO', 'ATE_5K', datetime(2026, 11, 1, 10), 'a.json'),
        ('c1', 'ATUALIZACAO', None, None, None, None,
         'Brasília', 'DF', 'ARROJADO', None, datetime(2026, 11, 2, 10), 'b.json'),
    ], 'id_cliente string, tipo_evento string, nome string, cpf string, email string, '
       'data_nascimento date, cidade string, uf string, perfil_investidor string, '
       'faixa_renda string, evento_em_utc timestamp, _arquivo_origem string')

    cliente = consolidar_clientes(eventos).first()

    assert cliente.nome == 'Ana Souza'
    assert cliente.cidade == 'Brasília'
    assert cliente.perfil_investidor == 'ARROJADO'
    assert cliente.faixa_renda == 'ATE_5K'