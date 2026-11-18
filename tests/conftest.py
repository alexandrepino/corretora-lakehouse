import pytest
from databricks.sdk.runtime import spark as _spark


@pytest.fixture(scope='session')
def spark():
    _spark.conf.set('spark.sql.session.timeZone', 'UTC')
    return _spark