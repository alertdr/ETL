from typing import Generator

from elasticsearch import helpers

from data_workers import PostgresLoader, ElasticsearchLoader, Filmwork
from etc.config import BATCH_SIZE
from etl.etc.queries import FULL_FILMWORKS
from state import State, JsonFileStorage
from utils import backoff


@backoff()
def extract_data(*, query: str) -> Generator:
        with PostgresLoader(fetch_size=BATCH_SIZE) as pg:
            for data in pg.batch_execute(query=query):
                for item in data:
                    yield item


def transform_data(*, data: Generator) -> Generator:
    for item in data:
        yield Filmwork(*item).get_es_format()


@backoff()
def load_data(*, data: Generator) -> None:
    with ElasticsearchLoader() as es:
        response = helpers.streaming_bulk(es, data, chunk_size=BATCH_SIZE, yield_ok=False)
        count = 1
        for k, v in response:
            print(count, k, v)
            if not k:
                pass


if __name__ == '__main__':
    storage_file = JsonFileStorage('state.json')
    state = State(storage_file)
    filmwork_query = FULL_FILMWORKS()
    data = extract_data(query=filmwork_query)
    pretty_data = transform_data(data=data)
    load_data(data=pretty_data)
