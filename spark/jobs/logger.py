import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


class Logger:
    def __init__(
        self,
        log_file: str = "logs/pipeline.log",
        backup_count: int = 5,
    ) -> None:
        """
        Initializes a logger that writes to console and rotates log files daily.

        :param log_file: Path to the log file (e.g., logs/pipeline.log)
        :param backup_count: Number of days to keep old log files
        """

        self.logger = logging.getLogger("CDCLogger")
        self.logger.setLevel(logging.DEBUG)

        if not self.logger.hasHandlers():
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)

            # File handler: rotate logs daily at midnight
            file_handler = TimedRotatingFileHandler(
                filename=log_file,
                when="midnight",
                interval=1,
                backupCount=backup_count,
                encoding="utf-8",
                utc=True,
            )

            file_formatter = logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(logging.DEBUG)

            # Console handler: prints INFO+ to stdout
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                "[%(levelname)s] %(message)s"
            )

            console_handler.setFormatter(console_formatter)
            console_handler.setLevel(logging.INFO)

            # Add handlers to logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def log(self, level: str, msg: str, *args) -> None:
        """
        Log a message at the specified level.

        Supports logging-style formatting, for example:
            logger.log("info", "Saved %s ports to %s", count, output_file)

        Levels:
            info
            warning
            error
            debug
            exception
        """

        level = level.lower()

        if args:
            msg = msg % args

        if level == "error":
            self.logger.error(msg)
        elif level == "warning":
            self.logger.warning(msg)
        elif level == "info":
            self.logger.info(msg)
        elif level == "debug":
            self.logger.debug(msg)
        else:
            self.logger.debug(msg)

    def info(self, msg: str, *args) -> None:
        """Log an INFO message."""
        self.logger.info(msg, *args)

    def warning(self, msg: str, *args) -> None:
        """Log a WARNING message."""
        self.logger.warning(msg, *args)

    def error(self, msg: str, *args) -> None:
        """Log an ERROR message."""
        self.logger.error(msg, *args)

    def debug(self, msg: str, *args) -> None:
        """Log a DEBUG message."""
        self.logger.debug(msg, *args)

    def exception(self, msg: str, *args) -> None:
        """Log an ERROR message and include the current exception traceback."""
        self.logger.exception(msg, *args)