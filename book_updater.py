import asyncio
import json
import random
import time
import traceback
import urllib.parse
from typing import Union, Tuple

from aiohttp import ClientResponse
from aiohttp.client import ClientSession

from abs_book_updater import AbstractBookUpdater, SyncBehavior
import aiohttp


class BookUpdater(AbstractBookUpdater):
    def __init__(self, api_path: str, user_name: str, pass_key: str):
        self.base_url: str = ''
        self.session: Union[ClientSession, None] = None
        self.namespace: str = api_path
        self.books_segment: str = ''
        self.cover_segment: str = ''
        self.genres_segment: str = ''
        self.book_volumes_segment: str = ''
        self.book_chapters_segment: str = ''
        self.volume_chapters_segment: str = ''
        self.headers: dict = {
            'Authorization': self.basic_auth(user_name, pass_key),
            'content-type': 'application/json'
        }
        self.max_retries: int = 3
        self.retry_delay: int = 3

    def get_routes(self, index_response: dict):
        self.books_segment = index_response["Book"]["segment"]
        self.cover_segment = index_response["Book"]["BookCover"]["segment"]
        self.genres_segment = index_response["Genre"]["segment"]
        self.book_volumes_segment = index_response["Book"]["Volume"]["segment"]
        self.book_chapters_segment = index_response["Book"]["BookChapter"]["segment"]
        self.volume_chapters_segment = index_response["Book"]["BookChapter"]["segment"]
        print("api schema:")
        print(self.genres_url())
        print(self.books_url())
        print(self.book_url(0))
        print(self.cover_url(0))
        print(self.volume_url(0))
        print(self.book_chapter_url(0))
        print(self.volume_chapter_url(0, 0))
        print()

    async def setup_host(self, scheme: str, host: str) -> bool:
        def assert_authorization(s: int) -> bool:
            if s == 401:
                print("401 Unauthorized")
                return False
            if s == 403:
                print("403 Forbidden")
                return False
            return True

        async def connection_test():
            return await self.fetch_data()
        try:
            self.base_url: str = "http://127.0.0.1"
            status, response = await connection_test()
            if status == 200:
                self.get_routes(response)
                print("setup localhost as host")
                return True

            if not assert_authorization(status):
                self.base_url = ''
                return False

            self.base_url = f"{scheme}://{host}"
            status, response = await connection_test()
            if status == 200:
                self.get_routes(response)
                print(f"setup {self.base_url} as host")
                return True

            assert_authorization(status)

            print("fail to setup a host")
            self.base_url = ''
            return False
        except Exception as e:
            print(repr(e))
            return False

    def books_url(self, title: str = None, author: dict = None) -> str:
        url = "/".join([self.books_segment])
        params = {}
        if title:
            params["title"] = title
        if author:
            params["author"] = author["name"]
        if params:
            return url + "?" + urllib.parse.urlencode(params)
        else:
            return url

    def book_url(self, book_id: int) -> str:
        """
        /books/{id}

        :param book_id:
        :return:
        """
        return "/".join([self.books_url(), str(book_id)])

    def cover_url(self, book_id: int) -> str:
        """
        /books/{id}/cover

        :param book_id:
        :return:
        """
        return "/".join([self.books_segment, str(book_id), self.cover_segment])

    def genres_url(self) -> str:
        """
        /genres

        :return:
        """
        return "/".join([self.genres_segment])

    def volume_url(self, book_id: int) -> str:
        """
        /books/{id}/volumes

        :param book_id:
        :return:
        """
        return "/".join([self.books_segment, str(book_id), self.book_volumes_segment])

    def book_chapter_url(self, book_id: int) -> str:
        """
        /books/{id}/chapters

        :param book_id:
        :return:
        """
        return "/".join([self.books_segment, str(book_id), self.book_chapters_segment])

    def volume_chapter_url(self, book_id: int, volume_id: int) -> str:
        """
        /books/{bid}/volumes/{vid}/chapters

        :param book_id:
        :param volume_id:
        :return:
        """
        return "/".join([self.volume_url(book_id), str(volume_id), self.volume_chapters_segment])

    @staticmethod
    async def json_result_from_response(response:  ClientResponse) -> Tuple[int, Union[dict, list, str]]:
        """
        try parse json result from response object

        :param response:
        :return: status code (9999 on failure) and response object
        """
        try:
            # print(str(response.url))
            # print(str(response.method))
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                return response.status, await response.json()
        except json.decoder.JSONDecodeError:
            pass
        return response.status, await response.text()

    async def __aenter__(self) -> "BookUpdater":
        self.create_session()
        return self

    async def __aexit__(self, exc_type: Exception, exc_val, err_traceback) -> None:
        await self.close_session()

    def create_session(self) -> None:
        if self.session is None:
            self.session = aiohttp.ClientSession(headers=self.headers)

    async def close_session(self) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None

    async def fetch_data(self, sub_path: str = None, method: str = 'GET', data: str = None)\
            -> Tuple[int, Union[dict, list, str]]:
        """
        :param sub_path: request url
        :param method: http method in string
        :param data: body serialized to string
        :return: status code (9999 on failure) and response object
        """
        # url = "/".join([self.base_url, self.namespace])
        # if sub_path:
        #     url += "/" + sub_path
        #
        # try:
        #     response: ClientResponse
        #     async with self.session.request(method, url, data=data) as response:
        #         return await self.json_result_from_response(response)
        #
        # except aiohttp.ClientResponseError as e:
        #     print("返回错误：", url, e)
        # except aiohttp.ClientError:
        #     print("连接错误：", url)
        # except aiohttp.ServerTimeoutError:
        #     print("服务器超时：", url)
        # except Exception as e:
        #     print("捕获了一个未知异常：", url, e)
        #     print(traceback.format_exc())
        #
        # return 9999, {}

        url = "/".join([self.base_url, self.namespace])
        if sub_path:
            url += "/" + sub_path

        for i in range(self.max_retries):
            if i != 0:
                duration = random.uniform(1, self.retry_delay) * i
                print(f"{duration}秒后进行第{i}次重试")
                time.sleep(duration)
            try:
                response: ClientResponse
                async with self.session.request(method, url, data=data) as response:
                    return await self.json_result_from_response(response)

            except aiohttp.ClientResponseError as e:
                print("返回错误：", url, e)
            except aiohttp.ClientError:
                print("连接错误：", url)
            except aiohttp.ServerTimeoutError:
                print("服务器超时：", url)
            except Exception as e:
                print("捕获了一个未知异常：", url, e)
                print(traceback.format_exc())

        return 9999, {}

    async def match_book(self, title: str, author: dict) -> Tuple[int, Union[dict, list, str]]:
        """
        search a book by title and author

        :param title: book title in string
        :param author: book author in string
        :return: status code and response object
        """
        return await self.fetch_data(self.books_url(title, author))

    async def get_book(self, book_id: int) -> Tuple[int, Union[dict, list, str]]:
        """
        get one book by id

        :param book_id: book
        :return: status code and response object
        """
        return await self.fetch_data(self.book_url(book_id))

    async def get_all_book_genres(self) -> Tuple[int, Union[dict, list, str]]:
        """
        get all valid book genres

        :return: status code and response object
        """
        return await self.fetch_data(self.genres_url())

    async def add_book(self, book_data: dict) -> Tuple[int, Union[dict, list, str]]:
        """
        create a book

        :param book_data: json dict represents a book
        :return: status code and response object
        """
        return await self.fetch_data(self.books_url(), 'POST', json.dumps(book_data))

    async def append_book_chapter(self, book_id: int, data: Union[dict, list]) -> Tuple[int, Union[dict, list, str]]:
        """
        append a chapter to the last volume of book

        :param book_id:
        :param data: json like dict represents a chapter, or a list of chapters represented in the same manner
        :return: status code and response object
        """
        return await self.fetch_data(self.book_chapter_url(book_id), 'POST', json.dumps(data))

    async def append_volume_chapter(self, book_id: int, volume_id: int, data: Union[dict, list])\
            -> Tuple[int, Union[dict, list, str]]:
        """
        append a chapter to given volume of book

        :param book_id:
        :param volume_id:
        :param data: json like dict represents a chapter, or a list of chapters represented in the same manner
        :return: status code and response object
        """
        return await self.fetch_data(self.volume_chapter_url(book_id, volume_id), 'POST', json.dumps(data))

    async def append_volume(self, book_id: int, data: dict) -> Tuple[int, Union[dict, list, str]]:
        """
        append a volume to the book

        :param book_id:
        :param data: json like dict represents a volume
        :return: status code and response object
        """
        return await self.fetch_data(self.volume_url(book_id), 'POST', json.dumps(data))

    async def submit_to_server(self):
        pass

    def post_book_info(self):
        pass

    def is_adjacent(self, a, b, collection):
        pass

    def merge_book_info(self, old_info, new_info, rm_behavior: SyncBehavior = SyncBehavior.MergeRemovedWithInertia):
        pass


if __name__ == '__main__':
    updater = BookUpdater('wp-json/kbp/v1')
    print(updater.setup_host('https', 'novelcabinet.lndo.site'))
    # asyncio.run(updater.get_book_info_server(365))
    print(asyncio.run(updater.get_all_book_genres()))

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
