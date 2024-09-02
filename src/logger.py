import logging
from datetime import datetime
from pathlib import Path


class Logger:
    def __init__(self, source: str):
        self.source = source
        date = datetime.today().strftime("%Y-%m-%d")
        base_path = Path("./logs_pipeline")
        logs_path = base_path / date

        base_path.mkdir(exist_ok=True)
        logs_path.mkdir(exist_ok=True)
        self.logger = logging.getLogger(self.source)
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        file_handler = logging.FileHandler(logs_path.joinpath(f"{self.source}.log"))
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)

    def log(self, level: str, message: str):
        if level.lower() == "info":
            self.logger.info(message)
        elif level.lower() == "warning":
            self.logger.warning(message)
        elif level.lower() == "error":
            self.logger.error(message)
        elif level.lower() == "debug":
            self.logger.debug(message)
        else:
            self.logger.info(message)


if __name__ == "__main__":
    logger = Logger("example_source")
    logger.log("info", "This is an info message.")
    logger.log("error", "This is an error message.")
