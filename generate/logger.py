import os
import logging
from logging import FileHandler
import psutil


class Logger:
    def __init__(self, name, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Check if handlers already exist to prevent duplicates
        if not self.logger.handlers:
            # Create a file handler that logs to a file
            log_file = name
            if not os.path.exists(os.path.dirname(log_file)):
                os.makedirs(os.path.dirname(log_file), exist_ok=True)
            if not os.path.exists(log_file):
                open(log_file, "a").close()
            file_handler = FileHandler(log_file)
            file_handler.setLevel(level)

            # Create a console handler for output to stderr
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)

            # Define the log format
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # Add the handlers to the logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

        # Log system stats only if no handlers were previously attached
        if len(self.logger.handlers) == 2:
            cpu_percent = psutil.cpu_percent()
            cpu_freq = psutil.cpu_freq()
            cpu_cores = psutil.cpu_count(logical=False)
            cpu_threads = psutil.cpu_count(logical=True)
            virtual_memory = psutil.virtual_memory()
            ram_size = virtual_memory.total
            # self.logger.info(f"CPU usage: {cpu_percent}%")
            self.logger.info(
                f"CPU frequency: {cpu_freq.max}Mhz, CPU cores: {cpu_cores}, CPU threads: {cpu_threads}, RAM size: {ram_size / (1024.0 ** 3)} GB"
            )
            # self.logger.info(f"Virtual memory: {virtual_memory}")

            disk_usage = psutil.disk_usage("/")
            total_space = disk_usage.total
            free_space = disk_usage.free
            used_space = total_space - free_space
            percent_used = (used_space / total_space) * 100
            # self.logger.info(f"Disk usage: {total_space}, {free_space}, {used_space}, {percent_used} %")

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
