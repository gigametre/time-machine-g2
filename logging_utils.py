import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


class JsonFormatter(logging.Formatter):
    """Formats log records as JSON objects (one per line)."""
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra context if provided via 'extra' parameter
        if hasattr(record, "context"):
            log_entry["context"] = record.context
            
        return json.dumps(log_entry)


class SessionLogger:
    """
    Manages structured logging for a specific session.
    Creates both session_results.log (all) and session_errors.log (errors only).
    """
    def __init__(self, session_dir: Path, name: str = "time_machine"):
        self.session_dir = session_dir
        self.session_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False  # Avoid duplicating logs to root logger
        
        # Clear existing handlers if re-initialized
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
            
        # 1. Main session log (INFO and above)
        results_path = self.session_dir / "session_results.log"
        results_handler = logging.FileHandler(results_path, encoding="utf-8")
        results_handler.setLevel(logging.INFO)
        results_handler.setFormatter(JsonFormatter())
        self.logger.addHandler(results_handler)
        
        # 2. Error-only log (ERROR and above)
        errors_path = self.session_dir / "session_errors.log"
        errors_handler = logging.FileHandler(errors_path, encoding="utf-8")
        errors_handler.setLevel(logging.ERROR)
        errors_handler.setFormatter(JsonFormatter())
        self.logger.addHandler(errors_handler)
        
        # 3. Console output (Standard format for local dev)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

    def info(self, msg: str, component: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        extra = {"context": context} if context else {}
        logger = logging.getLogger(component) if component else self.logger
        logger.info(msg, extra=extra)

    def error(self, msg: str, component: Optional[str] = None, context: Optional[Dict[str, Any]] = None, exc_info: bool = False):
        extra = {"context": context} if context else {}
        logger = logging.getLogger(component) if component else self.logger
        logger.error(msg, extra=extra, exc_info=exc_info)

    def warning(self, msg: str, component: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        extra = {"context": context} if context else {}
        logger = logging.getLogger(component) if component else self.logger
        logger.warning(msg, extra=extra)

    def debug(self, msg: str, component: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        extra = {"context": context} if context else {}
        logger = logging.getLogger(component) if component else self.logger
        logger.debug(msg, extra=extra)

    def log_data(self, data_type: str, data: Any, component: str = "data_capture"):
        """Special method for logging structured result data."""
        self.info(f"Captured {data_type} data", component=component, context={"data_type": data_type, "payload": data})


def get_session_logger(session_dir: Path) -> SessionLogger:
    """Helper to initialize a session logger."""
    return SessionLogger(session_dir)
