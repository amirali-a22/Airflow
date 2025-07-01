import os
import environ

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Load environment
env_name = environ.Env()
env_name.read_env(os.path.join(BASE_DIR, '.env'))
