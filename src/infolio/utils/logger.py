import logging
import os
import sys
from logging import Logger

import colorama

try:
    from pythonjsonlogger import jsonlogger
except Exception as exc:  # pragma: no cover
    raise ImportError(
        "python-json-logger is required for JSON logging. "
        "Install it with: pip install python-json-logger"
    ) from exc


class ColorFormatter(logging.Formatter):
    """
    Custom formatter that adds ANSI colors to log messages for console output.

    The color is determined based on the log level using the `colorama` package.

    Attributes
    ----------
    COLORS : dict[int, str]
        Mapping between log levels and corresponding color codes.
    """

    COLORS = {  # noqa: RUF012
        logging.DEBUG: colorama.Fore.CYAN,
        logging.INFO: colorama.Fore.GREEN,
        logging.WARNING: colorama.Fore.YELLOW,
        logging.ERROR: colorama.Fore.RED,
        logging.CRITICAL: colorama.Fore.RED + colorama.Style.BRIGHT,
    }

    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record with colors according to its severity level.

        Parameters
        ----------
        record : logging.LogRecord
            The log record containing information about the event being logged.

        Returns
        -------
        str
            The formatted log message with color applied.
        """
        log_fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")

        color = self.COLORS.get(record.levelno, "")
        message = formatter.format(record)
        return f"{color}{message}{colorama.Style.RESET_ALL}"


# ---- Configuration defaults ----
BASE_LOGGER_NAME = "infolio"

# Global state for one-time handler setup
_CONFIGURED: bool = False
_BASE_LOGGER: Logger | None = None


# ---- File handler factories ----
def make_file_handler(path: str, level: int, formatter: logging.Formatter) -> logging.Handler:
    """
    Create a basic non-rotating file handler.

    Parameters
    ----------
    path : str
        Path to the log file.
    level : int
        Logging level for the handler.
    formatter : logging.Formatter
        Formatter instance used to format log records.

    Returns
    -------
    logging.Handler
        Configured file handler instance.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    handler = logging.FileHandler(path)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    return handler


def make_size_rotating_file_handler(
    path: str, level: int, formatter: logging.Formatter,
    max_bytes: int = 10_000_000, backup_count: int = 5
) -> logging.Handler:
    """
    Create a file handler that rotates when the log file reaches a certain size.

    Parameters
    ----------
    path : str
        Path to the log file.
    level : int
        Logging level for the handler.
    formatter : logging.Formatter
        Formatter instance used to format log records.
    max_bytes : int, default=10_000_000
        Maximum file size (in bytes) before rotation occurs.
    backup_count : int, default=5
        Number of backup files to keep.

    Returns
    -------
    logging.Handler
        Configured rotating file handler.
    """
    from logging.handlers import RotatingFileHandler  # noqa: PLC0415
    os.makedirs(os.path.dirname(path), exist_ok=True)
    handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backup_count)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    return handler


def make_timed_rotating_file_handler(
    path: str, level: int, formatter: logging.Formatter,
    when: str = "midnight", interval: int = 1, backup_count: int = 7
) -> logging.Handler:
    """
    Create a file handler that rotates logs based on time intervals.

    Parameters
    ----------
    path : str
        Path to the log file.
    level : int
        Logging level for the handler.
    formatter : logging.Formatter
        Formatter instance used to format log records.
    when : str, default="midnight"
        Time interval specifier (e.g., 'S', 'M', 'H', 'D', 'midnight').
    interval : int, default=1
        Number of time units between rotations.
    backup_count : int, default=7
        Number of backup files to keep.

    Returns
    -------
    logging.Handler
        Configured time-based rotating file handler.
    """
    from logging.handlers import TimedRotatingFileHandler  # noqa: PLC0415
    os.makedirs(os.path.dirname(path), exist_ok=True)
    handler = TimedRotatingFileHandler(path, when=when, interval=interval, backupCount=backup_count, utc=True)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    return handler


def _to_level(level: int | str | None, default: int = logging.INFO) -> int:
    """
    Normalize a log level input into an integer value.

    Parameters
    ----------
    level : int, str, or None
        Logging level, can be a string (e.g., "DEBUG"), integer, or None.
    default : int, default=logging.INFO
        Default level to return if input is None or invalid.

    Returns
    -------
    int
        Numeric log level corresponding to the input or default.
    """
    if level is None:
        return default
    if isinstance(level, int):
        return level
    try:
        return getattr(logging, str(level).upper())
    except AttributeError:
        return default


def _get_file_handler_from_env(level: int, formatter: logging.Formatter) -> logging.Handler | None:
    """
    Build a file handler from environment configuration.

    The handler type and configuration parameters are read from environment
    variables (e.g., ``LOG_FILE_HANDLER``, ``LOG_FILE_PATH``).

    Parameters
    ----------
    level : int
        Logging level for the handler.
    formatter : logging.Formatter
        Formatter instance for formatting log records.

    Returns
    -------
    logging.Handler or None
        Configured file handler, or None if disabled.
    """
    handler_type = os.getenv("LOG_FILE_HANDLER", "none").lower()
    log_path = os.getenv("LOG_FILE_PATH", "logs/infolio.jsonl")

    if handler_type == "none":
        return None

    if handler_type == "file":
        return make_file_handler(log_path, level, formatter)

    if handler_type == "size":
        max_bytes = int(os.getenv("LOG_MAX_BYTES", "10000000"))
        backup_count = int(os.getenv("LOG_BACKUP_COUNT", "5"))
        return make_size_rotating_file_handler(log_path, level, formatter, max_bytes, backup_count)

    if handler_type == "timed":
        when = os.getenv("LOG_WHEN", "midnight")
        interval = int(os.getenv("LOG_INTERVAL", "1"))
        backup_count = int(os.getenv("LOG_BACKUP_COUNT", "7"))
        return make_timed_rotating_file_handler(log_path, level, formatter, when, interval, backup_count)

    return None


def configure_logging(
    *,
    name: str = BASE_LOGGER_NAME,
    level: int | str | None = None,
    file_handler: logging.Handler | None = None,
) -> Logger:
    """
    Configure and initialize the shared logging system.

    Sets up both console (colorized) and optional file (JSON) handlers.
    This function is safe to call multiple times; configuration will only
    be applied once globally.

    Parameters
    ----------
    name : str, default="infolio"
        Base logger name for the system.
    level : int or str, optional
        Logging verbosity level.
    file_handler : logging.Handler, optional
        Custom file handler. If ``None``, a handler may be built from
        environment variables.

    Environment Variables
    ---------------------
    LOG_LEVEL : str, default="INFO"
        Base logging level.
    LOG_FILE_HANDLER : {"none","file","size","timed"}, default="none"
        Type of file handler to use.
    LOG_FILE_PATH : str, default="logs/infolio.jsonl"
        Path to the log file.
    LOG_MAX_BYTES : int, default=10000000
        Maximum size before rotation (for size handler).
    LOG_BACKUP_COUNT : int, default=5
        Number of backup files (for rotating handlers).
    LOG_WHEN : str, default="midnight"
        Time rotation unit (for timed handler).
    LOG_INTERVAL : int, default=1
        Rotation frequency (for timed handler).

    Returns
    -------
    Logger
        The configured base logger instance.
    """
    global _CONFIGURED, _BASE_LOGGER  # noqa: PLW0603

    if _CONFIGURED and _BASE_LOGGER:
        return _BASE_LOGGER

    env_level = os.getenv("LOG_LEVEL", "INFO")
    eff_level = _to_level(level, default=_to_level(env_level, logging.INFO))

    base_logger = logging.getLogger(name)
    base_logger.setLevel(eff_level)
    base_logger.propagate = False

    # --- Console Handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(eff_level)
    console.setFormatter(ColorFormatter())
    base_logger.addHandler(console)

    # --- File Handler
    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(module)s %(funcName)s %(lineno)d "
            "%(process)d %(threadName)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if file_handler is not None:
        base_logger.addHandler(file_handler)
    else:
        env_handler = _get_file_handler_from_env(eff_level, formatter)
        if env_handler:
            base_logger.addHandler(env_handler)

    _CONFIGURED = True
    _BASE_LOGGER = base_logger
    return base_logger


def get_logger(name: str | None = None, level: int | str | None = None) -> Logger:
    """
    Retrieve a logger that propagates into the shared base logger.

    Parameters
    ----------
    name : str, optional
        Sub-logger name. If omitted, the base logger is returned.
    level : int or str, optional
        Log level for this logger. Defaults to the base logger's level.

    Returns
    -------
    Logger
        A logger instance bound to the base logging configuration.
    """
    global _BASE_LOGGER  # noqa: PLW0603

    if not _CONFIGURED or _BASE_LOGGER is None:
        _BASE_LOGGER = configure_logging()  # env-driven defaults

    if name in (None, "", BASE_LOGGER_NAME):
        return _BASE_LOGGER

    full_name = name if name.startswith(f"{BASE_LOGGER_NAME}.") else f"{BASE_LOGGER_NAME}.{name}"
    logger = logging.getLogger(full_name)
    logger.propagate = True
    logger.setLevel(_to_level(level, default=_BASE_LOGGER.level))
    return logger
