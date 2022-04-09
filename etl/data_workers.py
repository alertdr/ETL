import json
import logging
from dataclasses import dataclass
from typing import Generator, Optional, Union

import psycopg2
from elasticsearch import Elasticsearch
from psycopg2.extras import DictCursor

from etc.config import DSL, ES_CONFIG


class PostgresLoader:
    """
    Класс для подключения и выполнения запросов к бд postgresql
    """

    def __init__(self, *, fetch_size: int = 300):
        self.fetch_size = fetch_size

    def __enter__(self):
        logging.debug('Connecting to postgres')
        self.connection = psycopg2.connect(**DSL, cursor_factory=DictCursor)
        self.cursor = self.connection.cursor()
        logging.debug('Postgres connection complete')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.commit()
        self.connection.close()
        logging.debug('Postgres connection closed')

    def batch_execute(self, *, query: str) -> Generator:
        """
        Метод извлечения группы полей

        :param query: sql запрос
        :yield: rows: группа полей, результат выполнения запроса
        """
        self.cursor.execute(query)
        while rows := self.cursor.fetchmany(size=self.fetch_size):
            logging.info(f'Postgres executed: {len(rows)}')
            yield rows

    def get_filmwork_modified(self, *, filmwork_id: str) -> list:
        """
        Метод извлечения последнего времени изменения конкретного кинопроизведения по id

        :param filmwork_id: id кинопроизведения
        :return: результат извлечения времени последнего изменения конкретного кинопроизведения
        """
        self.cursor.execute(f'SELECT modified FROM film_work WHERE id = \'{filmwork_id}\'')
        return self.cursor.fetchone()


class ElasticsearchLoader:
    """
    Класс для конфигурирования и подключения к elasticsearch
    """

    def __init__(self):
        self.index_name = ES_CONFIG['index_name']
        self._mapping = self.load_settings(ES_CONFIG['movies_settings'])

    def __enter__(self) -> Elasticsearch:
        logging.debug('Connecting to elasticsearch')
        self.es = Elasticsearch(ES_CONFIG['hosts'])
        if not self.es.indices.exists(self.index_name):
            logging.debug(f'Elasticsearch index {self.index_name} does not exists')
            self.es.indices.create(index=self.index_name, body=self._mapping)
            logging.debug(f'Elasticsearch index {self.index_name} created')
        logging.debug(f'Elasticsearch connection complete')
        return self.es

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.es.close()
        logging.debug('Elasticsearch connection closed')

    @staticmethod
    def load_settings(file_path: str) -> dict:
        """
        Загрузка настроек для индекса movies

        :param file_path: путь к файлу настроек
        :return: словарь настроек
        """
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

    def get_bulk_format(self) -> dict:
        """
        Метод возврата словаря для bulk запроса elasticsearch

        :return: словарь для bulk запросов elasticsearch
        """
        return {
            '_index': ES_CONFIG['index_name'],
            '_id': self.id,
            '_source': {**self.__dict__}
        }
