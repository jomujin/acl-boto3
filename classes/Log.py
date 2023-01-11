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
