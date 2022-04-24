import logging
from collections import deque
from logging import config
from time import sleep
from typing import Generator

from elasticsearch import helpers

from data_workers import PostgresLoader, ElasticsearchLoader, Filmwork, Person, Genre
from etc.config import BATCH_SIZE, AWAIT_TIME, LOGGER_CONF_PATH, STATE_FILE_PATH
from etc.queries import QUERIES
from state import State, JsonFileStorage
from utils import backoff, get_format_time

tables = {
    'filmwork': Filmwork,
    'person': Person,
    'genre': Genre
}


@backoff()
def extract_data(*, table: str, table_state: dict) -> Generator:
    """
    Функция извлечения данных из postgresql

    :param table: название таблицы
    :yield: item: единичный результат выполнения sql запроса
    """
    with PostgresLoader(fetch_size=BATCH_SIZE) as pg_ids:
        for ids in pg_ids.batch_execute(query=QUERIES[f'{table}_ids'](**table_state)):
            pretty_ids = tuple([i.pop() for i in ids])

            with PostgresLoader(fetch_size=BATCH_SIZE) as pg_filmworks:
                for data in pg_filmworks.batch_execute(query=QUERIES[table](pretty_ids)):
                    for item in data:
                        yield item


def transform_data(*, data: Generator, table: str) -> Generator:
    """
    Функция форматирования сырого sql поля в требуемый elasticsearch

    :param table: название таблицы
    :param data: строка tuple - результат выполнения sql запроса
    :yield: dict: отформатированный словарь для bulk запроса Elasticsearch
    """
    for item in data:
        yield tables[table](*item).get_bulk_format()


# @backoff()
def load_data(*, data: Generator, table: str) -> None:
    """
    Функция загрузки данных в elasticsearch

    :param table: название таблицы
    :param data: начальное время повтора
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
                db_sate = {k: v.isoformat() for k, v in tables[table].get_db_state().items()}
                state.set_state(key=table, value=db_sate)


def main(*, table: str, table_state: dict) -> None:
    """
    Отказоустойчивая ETL функция
    """
    data = extract_data(table=table, table_state=table_state)
    pretty_data = transform_data(data=data, table=table)
    load_data(data=pretty_data, table=table)


if __name__ == '__main__':
    config.fileConfig(LOGGER_CONF_PATH)
    state = State(storage=JsonFileStorage(file_path=STATE_FILE_PATH))
    while True:
        # states = [state.get_state(key=table) for table in TABLES]
        for table in tables.keys():
            logging.info(f'Query {table} started')
            result_state = state.get_state(key=table)
            if result_state:
                table_state = {k: get_format_time(time=v) for k, v in result_state.items()}
            else:
                table_state = tables[table].get_db_state()
            main(table=table, table_state=table_state)
        logging.info(f'Wait time {AWAIT_TIME}')
        sleep(AWAIT_TIME)
