import logging
from collections import deque
from logging import config
from time import sleep
from typing import Generator

from elasticsearch import helpers

from data_workers import PostgresLoader, ElasticsearchLoader, Filmwork
from etc.config import BATCH_SIZE, AWAIT_TIME, TABLES
from etl.etc.queries import QUERIES
from state import State, JsonFileStorage
from utils import backoff, get_format_time


def extract_data() -> Generator:
    """
    Функция извлечения данных из postgresql, нет возможности применить декортаор backoff() так как
    генератор инициализируется только в transform_data. В случае возникновения ошибки в extract_data
    отловить её можно будет только в transform_data. Были предприняты попытки написать еще один декоратор
    для инициализации генератора, но они не увенчались успехом

    :yield: item: единичный результат выполнения sql запроса
    """
    with PostgresLoader(fetch_size=BATCH_SIZE) as pg_ids:
        for ids in pg_ids.batch_execute(query=QUERIES['get_ids'](filmwork_date, genre_date, person_date)):
            pretty_ids = tuple([i.pop() for i in ids])

            with PostgresLoader(fetch_size=BATCH_SIZE) as pg_filmworks:
                for data in pg_filmworks.batch_execute(query=QUERIES['filmwork'](pretty_ids)):
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


def load_data(*, data: Generator) -> None:
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
                    person_state, genre_state = Filmwork.get_db_states()
                    state.set_state(key='filmwork', value=modified.isoformat())
                    state.set_state(key='genre', value=genre_state.isoformat())
                    state.set_state(key='person', value=person_state.isoformat())


@backoff()
def main() -> None:
    """
    Отказоустойчивая ETL функция

    :param query: sql запрос
    """
    data = extract_data()
    pretty_data = transform_data(data=data)
    load_data(data=pretty_data)


if __name__ == '__main__':
    config.fileConfig("etc/logger.conf")
    state = State(storage=JsonFileStorage(file_path='state.json'))
    while True:
        logging.info(f'Query started')
        states = [state.get_state(key=table) for table in TABLES]
        filmwork_date, genre_date, person_date = [get_format_time(time=state_time) for state_time in states]
        main()
        logging.info(f'Wait time {AWAIT_TIME}')
        sleep(AWAIT_TIME)
