#!/usr/bin/env python3

import logging

global_level = logging.DEBUG
_global_handler = None  # Set the handler


def set_global_handler(filename):
    global _global_handler
    _global_handler = logging.FileHandler(filename)
    _global_handler.setLevel(global_level)
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)
    _global_handler.setFormatter(formatter)


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(global_level)

    if _global_handler is not None:
        logger.addHandler(_global_handler)
    else:
        raise ValueError("Handler must be set.")

    return logger
