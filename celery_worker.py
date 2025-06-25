#!/usr/bin/env python3
"""
Celery worker starter script
"""

import os
from dotenv import load_dotenv
from tasks import celery_app

# Load environment variables
load_dotenv()

if __name__ == '__main__':
    # Start the Celery worker
    celery_app.start()
