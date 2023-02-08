import logging
import logging.handlers
import sys

consoleLoggingLevel = logging.DEBUG

loggerName = 'logging'


logger = logging.getLogger(loggerName)
logger.setLevel(logging.DEBUG)


ch = logging.StreamHandler(sys.stdout)

# Set console logging level
ch.setLevel(consoleLoggingLevel)

formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')

ch.setFormatter(formatter)

# add handler for the logger
logger.addHandler(ch)

# sample logger to debug if logger is working or not.
logger.debug('Started logging')
