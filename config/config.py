from classes.DBConn import DBConn
from condition.config_cond import (
    AWS_AURORA_CONFIG
)


# postgres url variables from environment
AWS_PG_HOST = AWS_AURORA_CONFIG.get('ENDPOINT', False)
AWS_PG_PORT = AWS_AURORA_CONFIG.get('PORT', False)
AWS_PG_DBNAME = AWS_AURORA_CONFIG.get('DBNAME', False)
AWS_PG_USER = AWS_AURORA_CONFIG.get('USER', False)
AWS_PG_REGION = AWS_AURORA_CONFIG.get('REGION', False)
AWS_PG_PASSWORD = AWS_AURORA_CONFIG.get('PASSWORD', False)
assert AWS_PG_HOST  is not False
assert AWS_PG_USER is not False
assert AWS_PG_PORT == '5432'
assert AWS_PG_REGION is not False
assert AWS_PG_DBNAME is not False
assert AWS_PG_PASSWORD is not False

TEST_AWS_DB = DBConn(
    AWS_PG_HOST,
    AWS_PG_PORT,
    AWS_PG_USER,
    AWS_PG_PASSWORD,
    AWS_PG_DBNAME
)
