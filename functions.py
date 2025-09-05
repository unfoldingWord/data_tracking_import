# Contains all shared functions
import logging
import os

def get_logger():
  this_logger = logging.getLogger()

  if not this_logger.hasHandlers():
    c_handler = logging.StreamHandler()
    if os.getenv('STAGE', False) == 'dev':
      c_handler.setLevel(logging.DEBUG)
      this_logger.setLevel(logging.DEBUG)
    else:
      c_handler.setLevel(logging.INFO)
      this_logger.setLevel(logging.INFO)

    log_format = '%(asctime)s  %(levelname)-8s %(message)s'
    c_format = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
    c_handler.setFormatter(c_format)

    this_logger.addHandler(c_handler)

  return this_logger