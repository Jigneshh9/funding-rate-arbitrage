import logging
import inspect
try:
    from pubsub import pub
    _HAS_PUBSUB = True
except ImportError:
    _HAS_PUBSUB = False
from functools import wraps
import json
from datetime import datetime

class JSONFormatter(logging.Formatter):
    """Format log records as JSON for structured logging / log aggregation."""
    def format(self, record):
        log_entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'module': record.name,
            'message': record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            log_entry['exception'] = self.formatException(record.exc_info)
        return json.dumps(log_entry)

# Setup for the general application logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# File handler — human-readable format
app_handler = logging.FileHandler('app.log')
app_handler.setLevel(logging.INFO)
app_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
app_handler.setFormatter(app_formatter)
logger.addHandler(app_handler)

# JSON file handler — structured format for log aggregation
json_handler = logging.FileHandler('app_structured.log')
json_handler.setLevel(logging.INFO)
json_handler.setFormatter(JSONFormatter())
logger.addHandler(json_handler)

# Console handler — real-time visibility
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# Setup for the function tracker logger
function_logger = logging.getLogger("FunctionTracker")
function_tracker_handler = logging.FileHandler('functionTracker.log')
function_tracker_handler.setLevel(logging.DEBUG)
function_tracker_formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
function_tracker_handler.setFormatter(function_tracker_formatter)
function_logger.addHandler(function_tracker_handler)
function_logger.setLevel(logging.DEBUG)

if _HAS_PUBSUB:
    pub.setListenerExcHandler(logging.exception)

def setup_topics():
    pub.addTopicDefnProvider(TopicDefinitionProvider(), pub.TOPIC_TREE_FROM_CLASS)

class TopicDefinitionProvider:
    def getDefn(self, topicNameTuple):
        if topicNameTuple == ('opportunity_found',):
            return {'opportunity': "arbitrage opportunity found."}
        return None

def log_function_call(func):
    """
    A decorator to log function calls, making it easier to track the flow of the program,
    including the file name where the function is defined.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        module = inspect.getmodule(func)
        if module is not None and hasattr(module, '__file__'):
            file_name = module.__file__
            # Extract just the file name from the path for brevity
            file_name = file_name.split('/')[-1]
        else:
            file_name = 'Unknown'

        # Log entering and exiting messages with file name and function name
        function_logger.info(f"Entering {func.__name__} in {file_name}")
        result = func(*args, **kwargs)
        function_logger.info(f"Exiting {func.__name__} in {file_name}")
        return result
    return wrapper
