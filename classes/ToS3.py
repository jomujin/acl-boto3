import os
import subprocess
from classes.Log import (
    Log,
    get_export_csv_log,
    get_export_ddl_log
)
from condition.config_cond import (
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
        self.pnm = None
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
            destination_s3_file_path = f'/{self.dnm}/{self.snm}/{self.tnm}/{self.pnm}.csv'
        else:
            pull_tnm = f'{self.snm}.{self.tnm}'
            export_table_sql_line = f'select * from {pull_tnm}'
            destination_s3_file_path = f'/{self.dnm}/{self.snm}/{self.tnm}/{self.tnm}.csv'
        
        export_sql = f"""
        select * from aws_s3.query_export_to_s3(
            '{export_table_sql_line}', 
            aws_commons.create_s3_uri(
            '{self.s3nm}', 
            '{destination_s3_file_path}', 
            '{self.s3region}'
            ),
            options :='format csv, HEADER true'
        )
        """

        try:
            self.cur.execute(export_sql)
            res = self.cur.fetchall()
            rows_uploaded, files_uploaded, bytes_uploaded = res[0]
            # 출력 파라미터
            # OUT rows_uploaded bigint: 지정된 쿼리에 대해 Amazon S3에 성공적으로 업로드된 테이블 행 수
            # OUT files_uploaded bigint: Amazon S3에 업로드된 파일 수(파일은 약 6GB 크기로 생성, 생성된 각 추가 파일 이름에 _partXX가 추가됩니다. XX는 2, 3 등을 나타냄)
            # OUT bytes_uploaded bigint: Amazon S3에 업로드된 총 바이트 수
            msg = get_export_csv_log(
                dnm = self.dnm,
                pull_tnm = pull_tnm,
                s3nm = self.s3nm,
                destination_s3_file_path = destination_s3_file_path,
                files_uploaded = files_uploaded,
                rows_uploaded = rows_uploaded,
                bytes_uploaded = bytes_uploaded,
            )
            logger.info(msg)
        except:
            logger.error('Failed Export CSV Table')
            raise ValueError

    def dump_ddl_to_s3(self):
        logger = Log("EXPORT_DDL_LOG").stream_handler("INFO")

        if self.pnm:
            pull_tnm = f'{self.snm}.{self.pnm}'
            destination_s3_file_path = f's3://{self.s3nm}/{self.dnm}/{self.snm}/{self.tnm}/{self.pnm}_DDL.sql'
        else:
            pull_tnm = f'{self.snm}.{self.tnm}'
            destination_s3_file_path = f's3://{self.s3nm}/{self.dnm}/{self.snm}/{self.tnm}/{self.tnm}_DDL.sql'

        dump_success = 1
        sql = f"""pg_dump \
        -h {AWS_AURORA_CONFIG.get('ENDPOINT')} \
        -p {AWS_AURORA_CONFIG.get('PORT')} \
        -d {AWS_AURORA_CONFIG.get('DBNAME')} \
        -U {AWS_AURORA_CONFIG.get('USER')} \
        -w \
        -t {pull_tnm} \
        --schema-only | \
        aws s3 cp - {destination_s3_file_path}"""

        try:
            subprocess.run(
                sql,
                shell=True,
                env={**os.environ, "PGPASSWORD": f"{AWS_AURORA_CONFIG.get('PASSWORD')}"}
            )
        except:
            dump_success = 0

        if dump_success:
            msg = get_export_ddl_log(
                dnm = self.dnm,
                pull_tnm = pull_tnm,
                destination_s3_file_path = destination_s3_file_path
            )
            logger.info(msg)
        else:
            logger.error('Failed Export DDL Table')
            raise ValueError