#######################################################################################################################
#
#
#  	Project     	: 	TimeSeries Data generation via Python Application
#
#   File            :   utils.py
#
#   Description     :
#
#   Created     	:   22 November 2024
#
#   Notes           :   Python Logging Package:
#                   :   https://docs.python.com/3/library/logging.html
#                   :   https://realpython.com/python-logging/
#
########################################################################################################################
__author__      = "George Leonard"
__email__       = "georgelza@gmail.com"
__version__     = "3.0.0"
__copyright__   = "Copyright 2025, - G Leonard"


import logging

"""
Common Generic Logger setup, used by master loop for console and common file.
"""
def logger(filename, console_debuglevel, file_debuglevel, log_format=None):
    
    """
    Configures and returns a logger instance with console and file handlers.

    Args:
        filename (str): The name of the log file.
        console_debuglevel (int): Debug level for console output (0=DEBUG, 1=INFO, etc.).
        file_debuglevel (int): Debug level for file output (0=DEBUG, 1=INFO, etc.).
        log_format (str, optional): Custom log format string.
                                    Defaults to '%(asctime)s - %(levelname)s - %(processName)s - %(message)s'.

    Returns:
        logging.Logger: Configured logger instance.
    """
    
    logger = logging.getLogger(__name__)
    # Ensure handlers are not added multiple times if logger is called more than once
    
    if not logger.handlers:
        logger.setLevel(logging.DEBUG) # Set overall logger level to DEBUG to allow all messages to pass to handlers

        # Define default format if not provided
        default_console_format = '%(asctime)s - %(levelname)s - %(processName)s - %(message)s'
        default_file_format = '%(asctime)s - %(levelname)s - %(message)s'

        # Create console handler
        ch = logging.StreamHandler()
        # Use provided format or default for console
        console_formatter = logging.Formatter(log_format if log_format else default_console_format)
        ch.setFormatter(console_formatter)

        # Set console log level
        if console_debuglevel == 0:
            ch.setLevel(logging.DEBUG)
            
        elif console_debuglevel == 1:
            ch.setLevel(logging.INFO)
            
        elif console_debuglevel == 2:
            ch.setLevel(logging.WARNING)
            
        elif console_debuglevel == 3:
            ch.setLevel(logging.ERROR)
            
        elif console_debuglevel == 4:
            ch.setLevel(logging.CRITICAL)
            
        else:
            ch.setLevel(logging.INFO) # Default
            
        logger.addHandler(ch)

        # Create file handler
        fh = logging.FileHandler(filename)
        
        # Use provided format or default for file
        file_formatter = logging.Formatter(log_format if log_format else default_file_format)
        fh.setFormatter(file_formatter)

        # Set file log level
        if file_debuglevel == 0:
            fh.setLevel(logging.DEBUG)
            
        elif file_debuglevel == 1:
            fh.setLevel(logging.INFO)
            
        elif file_debuglevel == 2:
            fh.setLevel(logging.WARNING)
            
        elif file_debuglevel == 3:
            fh.setLevel(logging.ERROR)
            
        elif file_debuglevel == 4:
            fh.setLevel(logging.CRITICAL)
            
        else:
            fh.setLevel(logging.INFO)  # Default
            
        logger.addHandler(fh)

    return logger

# end logger
