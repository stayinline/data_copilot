import logging
import sys

from config import DEBUG

_logger = logging.getLogger("copilot")

if DEBUG:
    _logger.setLevel(logging.DEBUG)
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)-5s %(name)s:%(lineno)d  %(message)s",
        datefmt="%H:%M:%S",
    ))
    _logger.addHandler(_handler)


def get_logger(name: str) -> logging.Logger:
    if DEBUG:
        child = logging.getLogger("copilot." + name)
        if not child.handlers:
            child.setLevel(logging.DEBUG)
            child.propagate = False  # prevent double-printing (parent already has a handler)
            _handler = logging.StreamHandler(sys.stdout)
            _handler.setFormatter(logging.Formatter(
                "[%(asctime)s] %(levelname)-5s %(name)s:%(lineno)d  %(message)s",
                datefmt="%H:%M:%S",
            ))
            child.addHandler(_handler)
        return child
    return _logger
