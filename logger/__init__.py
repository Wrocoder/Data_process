import logging
import colorlog


def setup_colored_logger(logger_name='example'):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    formatter_string = (
        "{log_color}{levelname: <8}{reset} {white}{message}"
    )

    formatter = colorlog.ColoredFormatter(
        formatter_string,
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        },
        secondary_log_colors={},
        style='{'
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


LOGGER = setup_colored_logger()


# from logger import setup_colored_logger
# logger = setup_colored_logger()
# logger.debug("DEBUG MODE")
# logger.info("INFO MODE")
# logger.warning("WARNING MODE")
# logger.error("ERROR MODE")
# logger.critical("CRITICAL ERROR MODE")

