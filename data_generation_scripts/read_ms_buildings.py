import logging
import os
import sys
from datetime import datetime

class Logger:

    def __init__(self, log_file=None, log_level=logging.INFO):
        self.log_file = log_file or f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.log_level = log_level
        self._setup_logger()

    def _setup_logger(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Log to file
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Log to console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def get_logger(self):
        return self.logger


if __name__ == "__main__":
    logger = Logger().get_logger()
    logger.info("This is an information message.")
    logger.warning
