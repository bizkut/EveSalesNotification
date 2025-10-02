import nest_asyncio
nest_asyncio.apply()
import os
import sys
from celery import Celery
from celery.signals import worker_process_init
import database
import logging

# Add the project's root directory to the Python path
# This is necessary for the Celery worker to find the 'bot' module
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from log_config import setup_logging

@worker_process_init.connect
def init_worker(**kwargs):
    """Initializes database connection pool and logging for each worker process."""
    setup_logging()
    logging.info("Initializing database connection pool for celery worker...")
    database.initialize_pool()

# Get the broker URL from environment variables
# Default to a local Redis instance if not set, for development flexibility
broker_url = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
result_backend_url = os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')

# Initialize the Celery application
celery = Celery(
    'eve_market_bot_tasks',
    broker=broker_url,
    backend=result_backend_url,
    include=['tasks']  # Look for tasks in a file named tasks.py
)

# Optional Celery configuration
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    beat_schedule={
        'dispatch-character-polls': {
            'task': 'tasks.dispatch_character_polls',
            'schedule': 120.0,  # Run every 2 minutes
        },
        'dispatch-daily-overviews': {
            'task': 'tasks.dispatch_daily_overviews',
            'schedule': 86400.0,  # Run once every 24 hours
        },
        'check-new-characters': {
            'task': 'tasks.check_new_characters',
            'schedule': 30.0,  # Run every 30 seconds
        },
        'purge-deleted-characters': {
            'task': 'tasks.purge_deleted_characters',
            'schedule': 300.0,  # Run every 5 minutes
        },
    }
)

if __name__ == '__main__':
    celery.start()