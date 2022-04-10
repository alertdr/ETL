import json
import os

from dotenv import load_dotenv

load_dotenv()

DSL = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432)
}

ES_CONFIG = {
    'hosts': json.loads(os.environ.get('ES_HOSTS', '["127.0.0.1"]')),
    'index_name': os.environ.get('INDEX_NAME', 'movies'),
    'movies_settings': 'etc/movies_settings.json'
}

TABLES = ('filmwork', 'genre', 'person')
BATCH_SIZE = 500

AWAIT_TIME = 60
