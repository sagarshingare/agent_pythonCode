"""
Logging utilities for the Informatica Migration Agent
"""
import logging
import sys
from pathlib import Path
from typing import Optional


class MigrationLogger:
    """Centralized logging for migration system"""
    
    _loggers = {}
    
    @staticmethod
    def get_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
        """Get or create a logger instance"""
        if name in MigrationLogger._loggers:
            return MigrationLogger._loggers[name]
        
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler (if specified)
        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        
        MigrationLogger._loggers[name] = logger
        return logger


def get_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    """Convenience function to get logger"""
    return MigrationLogger.get_logger(name, log_file)
