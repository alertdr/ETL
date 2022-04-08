import json
from dataclasses import dataclass
from typing import Generator, Optional, Union

import psycopg2
from elasticsearch import Elasticsearch
from psycopg2.extras import DictCursor

from etc.config import DSL, ES_CONFIG


class PostgresLoader:
    def __init__(self, *, fetch_size: int = 300):
        self.fetch_size = fetch_size

    def __enter__(self):
        self.connection = psycopg2.connect(**DSL, cursor_factory=DictCursor)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.commit()
        self.connection.close()

    def batch_execute(self, *, query: str) -> Generator:
        self.cursor.execute(query)
        while rows := self.cursor.fetchmany(size=self.fetch_size):
            yield rows


class ElasticsearchLoader:
    def __init__(self):
        self.index_name = ES_CONFIG['index_name']
        self._mapping = self.load_mapping(ES_CONFIG['movies_settings'])

    def __enter__(self) -> Elasticsearch:
        self.es = Elasticsearch(ES_CONFIG['hosts'])
        if not self.es.indices.exists(self.index_name):
            self.es.indices.create(index=self.index_name, body=self._mapping)
        return self.es

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.es.close()

    @staticmethod
    def load_mapping(file_path: str) -> dict:
        with open(file_path, 'r') as file:
            return json.load(file)


@dataclass
class Filmwork:
    id: str
    title: str
    description: str
    imdb_rating: float
    genre: list[str]
    actors: Optional[Union[list[dict], dict]]
    director: list[str]
    writers: Optional[Union[list[dict], dict]]
    actors_names: Optional[list] = None
    writers_names: Optional[list] = None

    def __post_init__(self):
        if self.actors:
            self.actors_names = list(self.actors.keys())
            self.actors = [{'id': v, 'name': k} for k, v in self.actors.items()]

        if self.writers:
            self.writers_names = list(self.writers.keys())
            self.writers = [{'id': v, 'name': k} for k, v in self.writers.items()]

        if not self.director:
            self.director = []

    def get_es_format(self) -> dict:
        return {
            '_index': ES_CONFIG['index_name'],
            '_id': self.id,
            '_source': {**self.__dict__}
        }
