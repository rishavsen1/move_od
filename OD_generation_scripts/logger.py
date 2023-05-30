import os
import logging
from logging import FileHandler
import psutil


class Logger:
    def __init__(self, name, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Create a file handler that creates the file if it doesn't exist
        log_file = f"{name}.log"
        if not os.path.exists(os.path.dirname(log_file)):
            os.makedirs(os.path.dirname(log_file))
        if not os.path.exists(log_file):
            open(log_file, "a").close()
        file_handler = FileHandler(log_file)
        file_handler.setLevel(level)

        # Create a console handler for debugging purposes
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        # Define the log format
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # Log system stats
        cpu_percent = psutil.cpu_percent()
        cpu_freq = psutil.cpu_freq()
        cpu_cores = psutil.cpu_count(logical=False)
        cpu_threads = psutil.cpu_count(logical=True)
        virtual_memory = psutil.virtual_memory()
        ram_size = virtual_memory.total
        self.logger.info(f"CPU usage: {cpu_percent}%")
        self.logger.info(f"CPU frequency: {cpu_freq.current}Mhz")
        self.logger.info(f"CPU cores: {cpu_cores}")
        self.logger.info(f"CPU threads: {cpu_threads}")
        self.logger.info(f"RAM size: {ram_size / (1024.0 ** 3)} GB")  # Convert to GB
        self.logger.info(f"Virtual memory: {virtual_memory}")

        disk_usage = psutil.disk_usage("/")
        self.logger.info(f"Disk usage: {disk_usage}")

    def info(self, msg):
        self.logger.info(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

    def critical(self, msg):
        self.logger.critical(msg)
