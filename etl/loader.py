import logging
from collections import deque
from logging import config
from time import sleep
from typing import Generator

from elasticsearch import helpers

from data_workers import PostgresLoader, ElasticsearchLoader, Filmwork
from etc.config import BATCH_SIZE, AWAIT_TIME
from etl.etc.queries import QUERIES
from state import State, JsonFileStorage
from utils import backoff, get_format_time


def extract_data(*, query: str) -> Generator:
    """
    Функция извлечения данных из postgresql, нет возможности применить декортаор backoff() так как
    генератор инициализируется только в transform_data. В случае возникновения ошибки в extract_data
    отловить её можно будет только в transform_data. Были предприняты попытки написать еще один декоратор
    для инициализации генератора, но они не увенчались успехом

    :param query: sql запрос
    :yield: item: единичный результат выполнения sql запроса
    """
    with PostgresLoader(fetch_size=BATCH_SIZE) as pg:
        for data in pg.batch_execute(query=query):
            for item in data:
                yield item


def transform_data(*, data: Generator) -> Generator:
    """
    Функция форматирования сырого sql поля в требуемый elasticsearch

    :param data: строка tuple - результат выполнения sql запроса
    :yield: dict: отформатированный словарь для bulk запроса Elasticsearch
    """
    for item in data:
        yield Filmwork(*item).get_bulk_format()


def load_data(*, data: Generator, query_name: str) -> None:
    """
    Функция загрузки данных в elasticsearch

    :param data: начальное время повтора
    :param query_name: имя sql запроса необходимое для сохранения состояния последнего добавленного элемента
    """
    last_event = None
    with ElasticsearchLoader() as es:
        response = helpers.streaming_bulk(es, data, chunk_size=BATCH_SIZE)
        try:
            last_result = deque(response, maxlen=1)
            _, last_event = last_result.pop() if len(last_result) == 1 else (None, None)
        finally:
            logging.info('Loading is complete')
            if last_event:
                with PostgresLoader() as pg:
                    modified, = pg.get_filmwork_modified(filmwork_id=last_event['index']['_id'])
                    state.set_state(key=query_name, value=modified.isoformat())


@backoff()
def main(*, query_name: str, query: str) -> None:
    """
    Отказоустойчивая ETL функция

    :param data: начальное время повтора
    :param query_name: имя sql запроса необходимое для сохранения состояния последнего добавленного элемента
    """
    data = extract_data(query=query)
    pretty_data = transform_data(data=data)
    load_data(data=pretty_data, query_name=query_name)


if __name__ == '__main__':
    config.fileConfig("etc/logger.conf")
    state = State(storage=JsonFileStorage(file_path='state.json'))
    while True:
        for query_name, sql_query in QUERIES.items():
            logging.info(f'Query {query_name} started')
            time = get_format_time(time=state.get_state(key=query_name))
            query = sql_query(time)
            main(query_name=query_name, query=query)

        sleep(AWAIT_TIME)
