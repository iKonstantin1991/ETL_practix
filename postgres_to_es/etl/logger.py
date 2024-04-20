import logging


logger = logging.getLogger("etl")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)
