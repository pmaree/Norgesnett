import logging, os

PATH = os.path.dirname(__file__)
LOG_PATH = PATH + '/../logs/'

class bcolors:
    INFO = '\033[30m'
    WARNING = '\033[93m'
    EXCEPTION = '\033[91m'
    ENDC = '\033[0m'


log = lambda msg, color=bcolors.INFO: print(f"{color}{msg}{bcolors.ENDC}", logging.log(f"{color}{msg}{bcolors.ENDC}"))


class Logging:

    def __init__(self):
        if not os.path.exists(LOG_PATH):
            os.mkdir(LOG_PATH)

        logging.basicConfig(filename=LOG_PATH+'log', level=logging.INFO)

    def __log(self, msg:str, color: bcolors):
        print(f"{color}{msg}{bcolors.ENDC}")
        if color == bcolors.INFO:
            logging.info(msg)
        elif color == bcolors.WARNING:
            logging.warning(msg)
        elif color == bcolors.EXCEPTION:
            logging.exception(msg)

    def info(self, msg: str):
        self.__log(msg, bcolors.INFO)

    def warning(self, msg: str):
        self.__log(msg, bcolors.WARNING)

    def exception(self, msg: str):
        self.__log(msg, bcolors.EXCEPTION)