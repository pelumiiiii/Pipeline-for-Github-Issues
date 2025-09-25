import logging
import sys

def get_logger(name="pipeline"):
    log = logging.getLogger(name)
    if not log.handlers:
        log.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        log.addHandler(handler)
        log.propagate = False
    return log
