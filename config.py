import os
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)


class Settings:
    global_redis_host: str = os.environ.get("GLOBAL_REDIS_HOST", "redis://redis")
    global_redis_read_host: str = os.environ.get("GLOBAL_REDIS_READ_HOST", "redis://redis")
    redis_password: str = os.getenv("REDIS_PASSWORD", "redis_pass")
    redis_db: int = int(os.getenv("REDIS_DB", "0"))

