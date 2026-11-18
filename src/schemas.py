'''Schemas dos payloads da Bronze. Todos os campos numéricos são lidos como string.'''

SCHEMA_AWESOME_ULTIMA = (
    'map<string, struct<code: string, codein: string, name: string, high: string, low: string, '
    'varBid: string, pctChange: string, bid: string, ask: string, `timestamp`: string, create_date: string>>'
)

SCHEMA_AWESOME_DIARIO = (
    'array<struct<code: string, codein: string, high: string, low: string, varBid: string, '
    'pctChange: string, bid: string, ask: string, `timestamp`: string>>'
)

SCHEMA_AWESOME_CATALOGO = 'map<string, string>'

SCHEMA_BRAPI_COTACAO = (
    'struct<results: array<struct<symbol: string, shortName: string, currency: string, '
    'regularMarketPrice: string, regularMarketDayHigh: string, regularMarketDayLow: string, '
    'regularMarketChangePercent: string, regularMarketTime: string>>>'
)

SCHEMA_BRAPI_HISTORICO = (
    'struct<results: array<struct<symbol: string, historicalDataPrice: array<struct<'
    '`date`: string, open: string, high: string, low: string, close: string, volume: string>>>>>'
)

SCHEMA_BRAPI_CATALOGO = (
    'struct<stocks: array<struct<stock: string, name: string, sector: string, `type`: string>>, '
    'indexes: array<struct<stock: string, name: string>>>'
)

SCHEMA_BCB = 'array<struct<data: string, valor: string>>'