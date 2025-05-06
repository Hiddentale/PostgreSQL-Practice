import psycopg2
import logging
from pydantic import BaseModel
from ...config import development


logger = logging.getLogger(__name__)