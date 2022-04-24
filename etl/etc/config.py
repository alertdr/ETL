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
    'index_names': json.loads(
        os.environ.get('INDEX_NAMES', '{"movies": "movies", "persons": "persons", "genres": "genres"}')),
    'movies_settings': {
        'movies': 'etc/movies_settings.json',
        'persons': 'etc/persons_settings.json',
        'genres': 'etc/genres_settings.json'
    }
}

LOGGER_CONF_PATH = 'etc/logger.conf'
STATE_FILE_PATH = 'state.json'
BATCH_SIZE = 500

AWAIT_TIME = 60
