import os
from dotenv import load_dotenv
from classes.DBConn import DBConn


load_dotenv()
AWS_PG_HOST = os.environ['AWS_AURORA_PSQL_ENDPOINT']
AWS_PG_PORT = os.environ['AWS_AURORA_PSQL_PORT']
AWS_PG_DBNAME = os.environ['AWS_AURORA_PSQL_DBNAME']
AWS_PG_USER = os.environ['AWS_AURORA_PSQL_USER']
AWS_PG_REGION = os.environ['AWS_AURORA_PSQL_REGION']
AWS_PG_PASSWORD = os.environ['AWS_AURORA_PSQL_PASSWORD']
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
