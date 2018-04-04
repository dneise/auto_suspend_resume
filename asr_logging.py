import logging
import json_log_formatter

formatter = json_log_formatter.JSONFormatter()

json_handler = logging.FileHandler(filename='asr_log.json')
json_handler.setFormatter(formatter)

logger = logging.getLogger('')
logger.addHandler(json_handler)
logger.setLevel(logging.INFO)
