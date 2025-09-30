import os
from celery import Celery

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
)

if __name__ == '__main__':
    celery.start()