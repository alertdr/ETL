import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Generator, Optional, Union

import psycopg2
from elasticsearch import Elasticsearch
from psycopg2.extras import DictCursor

from etc.config import DSL, ES_CONFIG
from utils import latest_datetime_from_list, latest_datetime


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


class ElasticsearchLoader:
    """
    Класс для конфигурирования и подключения к elasticsearch
    """

    def __init__(self):
        self.index_name = ES_CONFIG['index_names']

    def __enter__(self) -> Elasticsearch:
        logging.debug('Connecting to elasticsearch')
        self.es = Elasticsearch(ES_CONFIG['hosts'])
        for index in self.index_name.values():
            if not self.es.indices.exists(index):
                logging.debug(f'Elasticsearch index {index} does not exists')
                mapping = self.load_settings(ES_CONFIG['movies_settings'][index])
                self.es.indices.create(index=index, body=mapping)
                logging.debug(f'Elasticsearch index {index} created')
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
    creation_date: datetime
    genre: Optional[Union[list[dict], dict]]
    actors: Optional[Union[list[dict], dict]]
    director: Optional[Union[list[dict], dict]]
    writers: Optional[Union[list[dict], dict]]
    person_time: list
    genre_time: list
    filmwork_time: datetime
    directors_names: Optional[list] = None
    actors_names: Optional[list] = None
    writers_names: Optional[list] = None
    genres_names: Optional[list] = None
    filmwork_latest_modified = None
    genre_latest_modified = None
    person_latest_modified = None

    def __post_init__(self):
        self.actors_names, self.actors = self.format_obj_agg(data=self.actors)
        self.writers_names, self.writers = self.format_obj_agg(data=self.writers)
        self.directors_names, self.director = self.format_obj_agg(data=self.director)
        self.genres_names, self.genre = self.format_obj_agg(data=self.genre)

        self.set_latest(fw_time=self.filmwork_time, person_time=self.person_time, genre_time=self.genre_time)

    @classmethod
    def set_latest(cls, *, fw_time, person_time, genre_time):
        cls.filmwork_latest_modified = latest_datetime(current=cls.filmwork_latest_modified, obj_time=fw_time)
        cls.person_latest_modified = latest_datetime_from_list(current=cls.person_latest_modified, obj_time=person_time)
        cls.genre_latest_modified = latest_datetime_from_list(current=cls.genre_latest_modified, obj_time=genre_time)

    def get_bulk_format(self) -> dict:
        """
        Метод возврата словаря для bulk запроса elasticsearch

        :return: словарь для bulk запросов elasticsearch
        """
        source = self.__dict__.copy()
        del source['person_time'], source['genre_time'], source['filmwork_time']
        if all((source.get('filmwork_latest_modified'),
                source.get('genre_latest_modified'),
                source.get('person_latest_modified'))):
            del source['filmwork_latest_modified'], source['genre_latest_modified'], source['person_latest_modified']
        return {
            '_index': ES_CONFIG['index_names']['movies'],
            '_id': self.id,
            '_source': {**source}
        }

    @classmethod
    def get_db_state(cls):
        return {
            'filmwork_date': cls.filmwork_latest_modified,
            'person_date': cls.person_latest_modified,
            'genre_date': cls.genre_latest_modified
        }

    @staticmethod
    def format_obj_agg(*, data: dict) -> tuple:
        """
        Функция форматирования агрегации объекта filmwork в postgresql

        :param data: форматируемый словарь
        :return: список наименований, словарь айди: наименование
        """
        if data:
            return list(data.keys()), [{'id': v, 'name': k} for k, v in data.items()]
        else:
            return None, None


@dataclass
class Person:
    id: str
    name: str
    roles: list | None
    films_as_actor: str | None
    films_as_director: str | None
    films_as_writer: str | None
    person_time: datetime
    filmwork_time: list
    filmwork_latest_modified = None
    person_latest_modified = None

    def __post_init__(self):
        self.set_latest(fw_time=self.filmwork_time, person_time=self.person_time)
        self.films_as_actor = self.films_as_actor.replace('{', '').replace('}', '').split(
            ',') if self.films_as_actor else None
        self.films_as_director = self.films_as_director.replace('{', '').replace('}', '').split(
            ',') if self.films_as_director else None
        self.films_as_writer = self.films_as_writer.replace('{', '').replace('}', '').split(
            ',') if self.films_as_writer else None

    @classmethod
    def set_latest(cls, *, fw_time, person_time):
        cls.filmwork_latest_modified = latest_datetime_from_list(current=cls.filmwork_latest_modified, obj_time=fw_time)
        cls.person_latest_modified = latest_datetime(current=cls.person_latest_modified, obj_time=person_time)

    def get_bulk_format(self) -> dict:
        """
        Метод возврата словаря для bulk запроса elasticsearch

        :return: словарь для bulk запросов elasticsearch
        """
        source = self.__dict__.copy()
        del source['person_time'], source['filmwork_time']
        if all((source.get('filmwork_latest_modified'), source.get('person_latest_modified'))):
            del source['filmwork_latest_modified'], source['person_latest_modified']
        return {
            '_index': ES_CONFIG['index_names']['persons'],
            '_id': self.id,
            '_source': {**source}
        }

    @classmethod
    def get_db_state(cls):
        return {
            'filmwork_date': cls.filmwork_latest_modified,
            'person_date': cls.person_latest_modified
        }


@dataclass
class Genre:
    id: str
    name: str
    description: str
    genre_time: datetime
    genre_latest_modified = None

    def __post_init__(self):
        self.set_latest(genre_time=self.genre_time)

    @classmethod
    def set_latest(cls, *, genre_time):
        cls.genre_latest_modified = latest_datetime(current=cls.genre_latest_modified, obj_time=genre_time)

    def get_bulk_format(self) -> dict:
        """
        Метод возврата словаря для bulk запроса elasticsearch

        :return: словарь для bulk запросов elasticsearch
        """
        source = self.__dict__.copy()
        del source['genre_time']
        if source.get('genre_latest_modified'):
            del source['genre_latest_modified']
        return {
            '_index': ES_CONFIG['index_names']['genres'],
            '_id': self.id,
            '_source': {**source}
        }

    @classmethod
    def get_db_state(cls):
        return {'genre_date': cls.genre_latest_modified}
