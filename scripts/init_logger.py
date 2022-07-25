"""

        Author: Aldair Leon
        Date: May 19th, 2022

"""

import logging
import os


# Logger
def log(logger_name: str) -> logging:
    """
        Logger format

    :param logger_name:
    :return:
    """
    str(os.path.basename(__file__))
    logger = logging.getLogger(logger_name)
    if not logger.handlers:
        log_format = '%(asctime)s [%(levelname)s] [%(filename)s] %(message)s'
        formatter = logging.Formatter(log_format)
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel("INFO")
    return logger
