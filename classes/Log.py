import logging


class Log:

    def __init__(self, name: str):
        self.log = logging.getLogger(name)
        self.log.propagate = True
        self.formatter = logging.Formatter("%(asctime)s | [%(levelname)s] | %(message)s",
                              "%Y-%m-%d %H:%M:%S")
        self.levels = {
            "DEBUG" : logging.DEBUG,
            "INFO" : logging.INFO,
            "WARNING" : logging.WARNING,
            "ERROR" : logging.ERROR,
            "CRITICAL" : logging.CRITICAL
        }
    
    def stream_handler(self, level: str):
        if len(self.log.handlers) > 0:
            return self.log # Logger already exists
        else:
            """
            level :
            > "DEBUG" : logging.DEBUG , 
            > "INFO" : logging.INFO , 
            > "WARNING" : logging.WARNING , 
            > "ERROR" : logging.ERROR , 
            > "CRITICAL" : logging.CRITICAL , 
            """
            self.log.setLevel(self.levels[level])
            streamHandler = logging.StreamHandler()
            streamHandler.setFormatter(self.formatter)
            self.log.addHandler(streamHandler)
            return self.log


def get_export_csv_log(
    database_name: str,
    pull_table_name: str,
    dest_s3_file_path: str,
    files_uploaded: int,
    rows_uploaded: int,
    bytes_uploaded: int
) -> str:
    return (f"""Successful Export {database_name}.{pull_table_name}.csv to AWS S3 {dest_s3_file_path} |
files_uploaded: {files_uploaded} File(s) | rows_uploaded: {rows_uploaded} Row(s) | bytes_uploaded: {bytes_uploaded/(10**6)} Mb""")


def get_export_ddl_log(
    database_name: str,
    pull_table_name: str,
    dest_s3_file_path: str
) -> str:
    return (f"Successful Export {database_name}.{pull_table_name} DDL to AWS S3 {dest_s3_file_path}")


def get_import_table_log(
    database_name: str,
    pull_table_name: str,
    source_s3_file_path: str,
    result_msg: str
) -> str:
    return (f"""Successful Import {database_name}.{pull_table_name} from AWS S3 {source_s3_file_path} |
result_msg : {result_msg}""")