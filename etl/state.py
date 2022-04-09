import abc
import json
import logging
import os
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        pass


class JsonFileStorage(BaseStorage):
    """
    Класс для хранения состояния в файле.
    """

    def __init__(self, *, file_path: Optional[str] = None):
        self.file_path = file_path

    def retrieve_state(self) -> dict:
        state = {}
        if self.file_path is not None and os.path.exists(self.file_path):
            with open(self.file_path, 'r') as file:
                state = file.readline()
            logging.debug(f'State: {state} retrieved')
            return json.loads(state)

        return state

    def save_state(self, *, state: dict) -> None:
        with open(self.file_path, 'w') as file:
            file.write(json.dumps(state))
        logging.debug(f'State: {state} saved')


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, *, storage: BaseStorage):
        self.storage = storage

    def set_state(self, *, key: str, value: Any) -> None:
        state = self.storage.retrieve_state()
        state[key] = value
        logging.debug(f'State query: {key}: {value} set')
        self.storage.save_state(state=state)

    def get_state(self, *, key: str) -> Any:
        return self.storage.retrieve_state().get(key)
