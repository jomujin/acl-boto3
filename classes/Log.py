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
    dnm: str,
    pull_tnm: str,
    s3nm: str,
    destination_s3_file_path: str,
    files_uploaded: int,
    rows_uploaded: int,
    bytes_uploaded: int
):
    return (f"""Successful Export {dnm}{pull_tnm}.csv to AWS S3 {s3nm}{destination_s3_file_path} |
files_uploaded: {files_uploaded} File(s) | rows_uploaded: {rows_uploaded} Row(s) | bytes_uploaded: {bytes_uploaded/(10**6)} Mb""")

def get_export_ddl_log(
    dnm: str,
    pull_tnm: str,
    destination_s3_file_path: str
):
    return (f"Successful Export {dnm}{pull_tnm} DDL to AWS S3 {destination_s3_file_path}")