import os
import subprocess
import botocore
import typing
from classes.Log import (
    Log,
    get_export_csv_log,
    get_export_ddl_log,
    get_import_table_log
)
from classes.DBConn import (
    DBConn
)
from classes.Client import (
    Boto3Client
)


class ToS3:

    def __init__(
        self, 
        host: str,
        port: str,
        user: str,
        password: str,
        database: str,
        aws_access_key_id: str, # aws access key id
        aws_secret_access_key: str, # aws secret access key
        aws_s3_name: str, # s3 bucket name
        aws_s3_region: str # s3 bucket region
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_s3_name = aws_s3_name
        self.aws_s3_region = aws_s3_region
        self.con = DBConn(
            host,
            port,
            user,
            password,
            database
        ).getPsypgConn()
        self.cur = self.con.cursor()
        self.session = Boto3Client(
            self.aws_access_key_id,
            self.aws_secret_access_key,
            self.aws_s3_name,
            self.aws_s3_region
        )


    def check_exist_file(
        self,
        file_path: str
    ) -> bool:
        try:
            self.session.s3_client.head_object(
                Bucket=self.aws_s3_name,
                Key=file_path
            )
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise


    def get_pnm(
        self,
        schema_name: str,
        table_name: str
    ) -> typing.Tuple[str, str or None]:
        """
        schema_name : schema name
        table_name : table name
        """

        sql = f"""
        select 
            inhrelid::regclass,
            inhparent::regclass 
        from pg_inherits 
        where inhrelid IN (
            select oid from pg_class 
            WHERE relname = '{table_name}' 
            AND relnamespace = (
                SELECT 
                    oid 
                FROM pg_namespace 
                WHERE nspname = '{schema_name}'
            )
        )
        """
        try:
            self.cur.execute(sql)
            res = self.cur.fetchall()
            if res:
                partition_name, table_name = res[0]
                table_name: str = table_name.replace(f'{schema_name}.', '')
                partition_name: str = partition_name.replace(f'{schema_name}.', '')
                return table_name, partition_name
            else:
                return table_name, None
        except:
            return table_name, None
            # print('Not Partitioning Table')


    def export_csv_to_s3(
        self, 
        database_name: str, # db name
        schema_name: str, # schema name
        table_name: str, # table name
        if_exist: str = "fail"
    ):
        """
        database_name : database name
        schema_name : schema name
        table_name : table name
        if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Drop the table before inserting new values.
            * append: Insert new values to the existing table.

        Raises
        ------
        ValueError
            When the table already exists and `if_exists` is 'fail' (the
            default).

        """
        # if_exist_arguments = {
        #     "fail" : ,
        #     'pass' : ,
        #     "append" : logging.INFO
        # }

        logger = Log("EXPORT_CSV_LOG").stream_handler("INFO")
        table_name, partition_name = self.get_pnm(self, schema_name, table_name)

        if partition_name:
            pull_table_name = f'{schema_name}.{partition_name}'
            export_table_sql_line = f'select * from {pull_table_name}'
            dest_s3_file_path = f'{database_name}/{schema_name}/{table_name}/{partition_name}.csv'
        else:
            pull_table_name = f'{schema_name}.{table_name}'
            export_table_sql_line = f'select * from {pull_table_name}'
            dest_s3_file_path = f'{database_name}/{schema_name}/{table_name}/{table_name}.csv'
        
        is_exist = self.check_exist_file(dest_s3_file_path)
        print(is_exist)

        sql = f"""
        select * from aws_s3.query_export_to_s3(
            '{export_table_sql_line}', 
            aws_commons.create_s3_uri(
            '{self.aws_s3_name}', 
            '/{dest_s3_file_path}', 
            '{self.aws_s3_region}'
            ),
            options :='format csv, HEADER true'
        )
        """

        try:
            self.cur.execute(sql)
            res = self.cur.fetchall()
            rows_uploaded, files_uploaded, bytes_uploaded = res[0]
            # 출력 파라미터
            # OUT rows_uploaded bigint: 지정된 쿼리에 대해 Amazon S3에 성공적으로 업로드된 테이블 행 수
            # OUT files_uploaded bigint: Amazon S3에 업로드된 파일 수(파일은 약 6GB 크기로 생성, 생성된 각 추가 파일 이름에 _partXX가 추가됩니다. XX는 2, 3 등을 나타냄)
            # OUT bytes_uploaded bigint: Amazon S3에 업로드된 총 바이트 수
            msg = get_export_csv_log(
                database_name = database_name,
                pull_table_name = pull_table_name,
                dest_s3_file_path = dest_s3_file_path,
                files_uploaded = files_uploaded,
                rows_uploaded = rows_uploaded,
                bytes_uploaded = bytes_uploaded,
            )
            logger.info(msg)
        except:
            logger.error('Failed Export CSV Table')
            raise ValueError


    def dump_ddl_to_s3(
        self,
        database_name: str, # db name
        schema_name: str, # schema name
        table_name: str, # table name
        if_exist: str = "fail"
    ):
        """
        database_name : database name
        schema_name : schema name
        table_name : table name
        if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Drop the table before inserting new values.
            * append: Insert new values to the existing table.

        Raises
        ------
        ValueError
            When the table already exists and `if_exists` is 'fail' (the
            default).

        """
        # if_exist_arguments = {
        #     "fail" : ,
        #     'pass' : ,
        #     "append" : logging.INFO
        # }

        logger = Log("EXPORT_DDL_LOG").stream_handler("INFO")
        table_name, partition_name = self.get_pnm(
            self, 
            schema_name, 
            table_name
        )

        if partition_name:
            pull_table_name = f'{schema_name}.{partition_name}'
            dest_s3_file_path = f's3://{self.aws_s3_name}/{database_name}/{schema_name}/{table_name}/{partition_name}_DDL.sql'
        else:
            pull_table_name = f'{schema_name}.{table_name}'
            dest_s3_file_path = f's3://{self.aws_s3_name}/{database_name}/{schema_name}/{table_name}/{table_name}_DDL.sql'

        is_exist = self.check_exist_file(dest_s3_file_path)
        print(is_exist)

        dump_success = 1
        sql = f"""pg_dump \
        -h {self.host} \
        -p {self.port} \
        -d {self.database} \
        -U {self.user} \
        -w \
        -t {pull_table_name} \
        --schema-only | \
        aws s3 cp - {dest_s3_file_path}"""

        try:
            subprocess.run(
                sql,
                shell=True,
                env={**os.environ, "PGPASSWORD": f"{self.password}"}
            )
        except:
            dump_success = 0

        if dump_success:
            msg = get_export_ddl_log(
                database_name = database_name,
                pull_table_name = pull_table_name,
                dest_s3_file_path = dest_s3_file_path
            )
            logger.info(msg)
        else:
            logger.error('Failed Export DDL Table')
            raise ValueError


    def get_all_multi_part_list(
        self,
        file_path: str
    ) -> typing.List[str]:
        num = 1
        multi_part_list = list()

        while True:
            if num < 2:
                key_path = file_path
            else:
                key_path = file_path.replace('.csv', f'.csv_part{num}')

            try:
                self.session.s3_client.head_object(
                    Bucket=self.aws_s3_name,
                    Key=key_path
                )
                multi_part_list.append(key_path)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    break

            num += 1

        if len(multi_part_list):
            return multi_part_list
        else:
            raise ValueError


    def import_table_from_s3(
        self,
        import_database_name: str,
        import_schema_name: str,
        import_table_name: str,
        file_path: str,
        format: str = 'csv',
        header: str = 'true',
    ):
        """
        import_schema_name : A required text string containing the name of the PostgreSQL database schema to import the data into
        import_table_name : A required text string containing the name of the PostgreSQL database table to import the data into
        file_path : 
        format : 
        header :
        """

        multi_part_list = self.get_all_multi_part_list(file_path)
        if multi_part_list:
            
            logger = Log("EXPORT_CSV_LOG").stream_handler("INFO")
            
            for idx, multi_part_path in enumerate(multi_part_list):
                if idx < 3:
                    sql = f"""
                    SELECT aws_s3.table_import_from_s3(
                        '{import_schema_name}.{import_table_name}',
                        '', 
                        '(format {format}, HEADER {header})',
                        aws_commons.create_s3_uri(
                            '{self.aws_s3_name}',
                            '/{multi_part_path}',
                            '{self.aws_s3_region}'
                        )
                    )
                    """

                    try:
                        self.cur.execute(sql)
                        res = self.cur.fetchall()
                        if res:
                            msg = get_import_table_log(
                                database_name = import_database_name,
                                pull_table_name = f'{import_schema_name}.{import_table_name}',
                                source_s3_file_path = f's3://{self.aws_s3_name}/{multi_part_path}',
                                result_msg = res[0][0]
                            )
                            logger.info(msg)
                    except:
                        logger.error('Failed Import Table')
                        raise ValueError
                else:
                    break