import logging
import sys

def configure_logging(filename=None, filemode=None, fmt=None, datefmt=None,
        level=logging.INFO, stream=None, handler=None):
    if handler is None:
        if filename is None:
            handler = logging.StreamHandler(stream or sys.stderr)
        else:
            handler = logging.FileHandler(filename, filemode or 'a')

    if fmt is None:
        fmt = "[%(asctime)s] %(name)s/%(levelname)s | %(message)s"
    handler.setFormatter(logging.Formatter(fmt))

    log = logging.getLogger("divconq")
    log.setLevel(level)
    log.addHandler(handler)
