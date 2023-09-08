from abc import ABC, abstractmethod
from enum import Enum
from base64 import b64encode


class SyncBehavior(Enum):
    DiscardRemoved = 0,
    MergeRemovedStickily = 1,
    MergeRemovedWithInertia = 2,


class AbstractBookUpdater(ABC):
    @abstractmethod
    def post_book_info(self):
        pass

    @abstractmethod
    def is_adjacent(self, a, b, collection):
        pass

    @abstractmethod
    def merge_book_info(self, old_info, new_info, rm_behavior: SyncBehavior = SyncBehavior.MergeRemovedWithInertia):
        pass

    def basic_auth(self, username, password):
        '''
        https://stackoverflow.com/questions/6999565/python-https-get-with-basic-authentication
        :param password:
        :return:
        '''
        token = b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")
        return f'Basic {token}'


