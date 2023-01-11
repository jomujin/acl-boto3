import os
import subprocess
from classes.Log import Log
from condition.config_condition import (
    AWS_AURORA_CONFIG
)


class ToS3:

    def __init__(
        self, 
        con, # db Connection
        dnm: str, # db name
        snm: str, # schema name
        tnm: str, # table name
        s3nm: str, # destination s3 bucket name
        s3region: str # destination s3 bucket region
    ):
        self.con = con
        self.cur = con.cursor()
        self.dnm = dnm
        self.snm = snm
        self.tnm = tnm
        self.s3nm = s3nm
        self.s3region = s3region

    def get_pnm(self):
        get_pnm_sql = f"""
        select 
            inhrelid::regclass,
            inhparent::regclass 
        from pg_inherits 
        where inhrelid IN (
            select oid from pg_class 
            WHERE relname = '{self.tnm}' 
            AND relnamespace = (SELECT oid FROM pg_namespace 
            WHERE nspname = '{self.snm}'))
        """
        try:
            self.cur.execute(get_pnm_sql)
            res = self.cur.fetchall()
            if res:
                pnm, tnm = res[0]
                self.tnm: str = tnm.replace(f'{self.snm}.', '')
                self.pnm: str = pnm.replace(f'{self.snm}.', '')
        except:
            pass
            # print('Not Partitioning Table')

    def export_csv_to_s3(self):
        logger = Log("EXPORT_CSV_LOG").stream_handler("INFO")

        if self.pnm:
            pull_tnm = f'{self.snm}.{self.pnm}'
            export_table_sql_line = f'select * from {pull_tnm}'
            destination_aws_key_space = f'/{self.dnm}/{self.snm}/{self.tnm}/{self.pnm}.csv'
        else:
            pull_tnm = f'{self.snm}.{self.tnm}'
            export_table_sql_line = f'select * from {pull_tnm}'
            destination_aws_key_space = f'/{self.dnm}/{self.snm}/{self.tnm}/{self.tnm}.csv'
        
        export_sql = f"""
        select * from aws_s3.query_export_to_s3(
            '{export_table_sql_line}', 
            aws_commons.create_s3_uri(
            '{self.s3nm}', 
            '{destination_aws_key_space}', 
            '{self.s3region}'),
            options :='format csv, HEADER true'
        )
        """

        try:
            self.cur.execute(export_sql)
            logger.info(f"Success Export {self.dnm}{pull_tnm}.csv to AWS S3 {self.s3nm}{destination_aws_key_space}")
        except:
            logger.error('Failed Export CSV Table')
            raise ValueError

    def dump_ddl_to_s3(self):
        logger = Log("EXPORT_DDL_LOG").stream_handler("INFO")

        if self.pnm:
            pull_tnm = f'{self.snm}.{self.pnm}'
            destination_aws_key_space = f's3://{self.s3nm}/{self.dnm}/{self.snm}/{self.tnm}/{self.pnm}_DDL.sql'
        else:
            pull_tnm = f'{self.snm}.{self.tnm}'
            destination_aws_key_space = f's3://{self.s3nm}/{self.dnm}/{self.snm}/{self.tnm}/{self.tnm}_DDL.sql'

        dump_success = 1
        sql = f"""pg_dump \
        -h {AWS_AURORA_CONFIG.get('ENDPOINT')} \
        -p {AWS_AURORA_CONFIG.get('PORT')} \
        -d {AWS_AURORA_CONFIG.get('DBNAME')} \
        -U {AWS_AURORA_CONFIG.get('USER')} \
        -w \
        -t {pull_tnm} \
        --schema-only | \
        aws s3 cp - {destination_aws_key_space}"""

        try:
            proc = subprocess.Popen(
                sql,
                shell=True,
                env={**os.environ, "PGPASSWORD": f"{AWS_AURORA_CONFIG.get('PASSWORD')}"}
            )
            proc.wait()
        except:
            dump_success = 0

        if dump_success:
            logger.info(f"Success Export {self.dnm}{pull_tnm} DDL to AWS S3 {destination_aws_key_space}")
        else:
            logger.error('Failed Export DDL Table')
            raise ValueError