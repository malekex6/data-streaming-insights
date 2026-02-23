"""Structured logging configuration for Dataflow pipeline."""

import logging
import sys
import json
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """Format logs as JSON for Cloud Logging integration."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add custom fields
        if hasattr(record, "custom_fields"):
            log_data.update(record.custom_fields)
        
        return json.dumps(log_data)


def configure_logging(log_level: str = "INFO") -> None:
    """Configure structured logging for the Dataflow pipeline."""
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add JSON formatter to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)


def get_logger(name: str) -> logging.LoggerAdapter:
    """Get a logger with custom fields support."""
    logger = logging.getLogger(name)
    return logging.LoggerAdapter(logger, {})
