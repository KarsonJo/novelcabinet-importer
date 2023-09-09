import asyncio
import re
from abc import ABC, abstractmethod
from typing import Tuple, Union

from book_updater import BookUpdater
from helpers.logger import Logger, eprint


class AbsBookCrawler(ABC):
    def __init__(self):
        self.genre_mapping: dict = {}
        self.api_genres: dict = {}
        self.book_updater: Union[BookUpdater, None] = None

    async def __aenter__(self) -> "AbsBookCrawler":
        return self

    async def __aexit__(self, exc_type: Exception, exc_val, err_traceback) -> None:
        await self.close_updater()

    async def setup_updater(self,
                            user_name: str,
                            pass_key: str,
                            schema: str = 'https',
                            host: str = 'novelcabinet.lndo.site',
                            base_path: str = 'wp-json/kbp/v1') -> "AbsBookCrawler":
        """
        与api服务器建立会话连接
        :param user_name:
        :param pass_key:
        :param schema:
        :param host:
        :param base_path:
        :return:
        """
        self.book_updater = BookUpdater(base_path, user_name, pass_key)
        self.book_updater.create_session()

        if await self.book_updater.setup_host(schema, host) is False:
            await self.close_updater()
            raise ConnectionError(f"cannot setup api connection to: {schema}://{host}")

        if await self.fetch_genres() is False:
            print("获取类型失败")

        return self

    async def close_updater(self):
        await self.book_updater.close_session()

    async def fetch_genres(self) -> bool:
        [status, genres] = await self.book_updater.get_all_book_genres()
        if status == 200:
            self.api_genres = {item["name"]: item["id"] for item in genres}
            return True
        return False

    async def incremental_insert(self, batch_size: int = 50, logger: "Logger" = None) -> bool:
        """
        incremental insert volumes and chapters
        all volumes will be appended to the end of the book
        all chapters will be appended to the end of the volume

        :return:
        """
        if logger:
            logger.register_context("step")
            logger.register_context("progress")
            logger.register_context("function")

        if self.book_updater is None:
            raise TypeError("book updater not initialized")

        async with self.book_updater as updater:
            # 尝试获取基本信息
            basic_info = await self._get_book_basic_info()
            status, books = await updater.match_book(basic_info["title"], basic_info["author"])

            # success, message = self.check_response(status, books, "search book")
            # if not success:
            if status >= 400:
                # print(message)
                if logger:
                    logger.write_err_log(books, "search book")
                else:
                    self.__basic_error_log(books, "search book")
                return False

            # 尝试创建书籍
            if type(books) is list and len(books) != 0:
                # 查询到的第一条
                book: dict = books[0]

                logger and logger.add_log("steps", "book", f"found: id={book['id']}", "step")
            else:
                status, result = await updater.add_book({
                    "title": basic_info["title"],
                    "author": basic_info["author"],
                    "excerpt": self._string_para_strip(basic_info["excerpt"]),
                    "genres": self.__genre_map(basic_info["genres"]),
                    "tags": basic_info["tags"],
                    "volumes": [],
                })

                # 创建失败 出问题
                # [success, message] = self.check_response(status, result, "create")
                # if not success:
                if status >= 400:
                    # print(message)
                    if logger:
                        logger.write_err_log(result, "book")
                    else:
                        self.__basic_error_log(result, "create book")
                    return False
                # 创建结果的“data”字段
                # print(result)
                book: dict = result['data']

                logger and logger.add_log("steps", "book", f"inserted, id={book['id']}", "step")


            # print(book)
            # 获取目标的目录
            contents_info = await self._get_contents_info()

            # 记录volume title -> id的映射
            volume_id_map = self.__parse_volume_id_map(book)

            # 插入volumes
            volume_counter = 0
            for volume in contents_info["volumes"]:
                if volume["title"] not in volume_id_map:

                    [status, json_data] = await updater.append_volume(book['id'], {"title": volume["title"]})

                    # [success, message] = self.check_response(status, json_data, "create volume")
                    # if not success:
                    if status >= 400:
                        # print(message)
                        if logger:
                            logger.write_err_log(json_data, "volume")
                        else:
                            self.__basic_error_log(json_data, "create volume")
                        return False

                    volume_counter += 1
                    # 登记新插入的volume title -> id，后面会用到
                    volume_id_map[volume['title']] = json_data["data"]["id"]

            logger and logger.add_log("summary", "insert", f"book & volumes", "progress")
            if volume_counter > 0:
                logger and logger.add_log("steps", "volume", f"{volume_counter} inserted", "step")

            # 记录chapter title -> id的映射
            chapter_id_map = self.__parse_chapter_id_map(book)

            # 插入chapters
            for volume in contents_info["volumes"]:
                chapters = volume["chapters"]

                for i in range(0, len(chapters), batch_size):
                    batch = chapters[i:i + batch_size]
                    chapter_data = []

                    for chapter in batch:
                        if volume["title"] not in chapter_id_map \
                                or chapter["title"] not in chapter_id_map[volume["title"]]:
                            chapter_data.append({
                                "title": chapter["title"],
                                "content": self._string_to_html_p(await self._get_one_chapter(chapter["srcIdx"]))
                            })

                    if len(chapter_data) > 0:
                        # print(volume)
                        book_id = book["id"]
                        volume_id = volume_id_map[volume["title"]]
                        # print(f"{book_id} {volume_id} {chapter_data}")
                        status, data = await updater.append_volume_chapter(book_id, volume_id, chapter_data)

                        # success, message = self.check_response(status, data, "create chapters")
                        # if not success:
                        if status >= 400:
                            # print(message)
                            if logger:
                                logger.write_err_log(data, "volume")
                            else:
                                self.__basic_error_log(data, "create chapters")
                            return False

                        logger and logger.add_log("steps", "chapter", f"{len(chapter_data)} inserted", "step")

            # print(f"book inserted: {book['id']}")
            logger and logger.add_log("summary", "insert", f"chapters", "progress")
            logger and logger.add_log("summary", "done", f"book id={book['id']}", "function")
            logger and logger.write_logs()
            return True

    @staticmethod
    def check_response(status: int, data: Union[dict, list, str], step_name: str = "") -> Tuple[bool, str]:
        """
        check if response is successful

        :param status: response status code
        :param data: response data, json or plain string
        :param step_name: used to wrap message
        :return: bool status and string report message
        """
        if status > 400:
            err_str = "[error]"
            if step_name != "":
                err_str += f" [{step_name}]"
            err_str += f": {repr(data)}"
            return False, err_str
        else:
            return True, ""

    @staticmethod
    def __basic_error_log(self, data: Union[dict, list, str], step_name: str = "") -> None:
        err_str = "[error]"
        if step_name != "":
            err_str += f" [{step_name}]"
        err_str += f": {repr(data)}"
        eprint(err_str)

    @staticmethod
    def __parse_volume_id_map(book: dict) -> dict:
        """
        result["volume-title"] = vid

        :return:
        """
        result = {}
        for volume in book["volumes"]:
            result[volume["title"]] = volume["id"]
        return result

    @staticmethod
    def __parse_chapter_id_map(book: dict) -> dict:
        """
        result["volume-title"]["chapter-title] = cid

        :return:
        """
        result = {}
        for volume in book["volumes"]:
            result[volume["title"]] = {}
            for chapter in volume["chapters"]:
                result[volume["title"]][chapter["title"]] = chapter["id"]
        return result

    @staticmethod
    def _string_para_strip(input_string: str):
        """
        strip empty space every line, preserve at most one empty line between paragraphs
        :param input_string:
        :return:
        """
        input_string = re.sub(r"\r\n|\n|\r", "\n", input_string).strip()
        paras = re.split(r"\n\n+", input_string)
        return "\n\n".join(["\n".join([line.strip() for line in para.split("\n")]) for para in paras])

    @staticmethod
    def _string_to_html_p(input_string: str):
        # 使用生成器表达式和filter筛选非空行并包装在<p>标签中
        paragraphs_generator = (f"<p>{line.strip()}</p>" for line in input_string.split('\n') if line.strip())

        # 使用'\n'连接生成器中的内容，而不产生额外的字符串
        result = '\n'.join(paragraphs_generator)

        return result

    def add_genres_mapping_rule(self, outer, inner):
        self.genre_mapping[outer] = inner

    def __genre_map(self, outer_genres: str):
        """
        将一个字符串转换：从“外部类别”=>“api类别”=>“id”
        :param outer_genres:
        :return:
        """
        def mp(genre):
            if genre in self.genre_mapping and self.genre_mapping[genre] in self.api_genres:
                return self.api_genres[self.genre_mapping[genre]]
            return ""

        return list(filter(lambda x: x != "", map(mp, outer_genres)))

    @abstractmethod
    async def _get_book_basic_info(self):
        pass

    @abstractmethod
    async def _get_contents_info(self):
        """
        volumes: dict
            title: str
            chapters: dict
                title: str
                srcIdx: any (all info needed to retrieve a chapter)

        :return:
        """
        pass

    @abstractmethod
    async def _get_one_chapter(self, src_idx):
        """

        :param src_idx: all info needed to retrieve a chapter
        :return:
        """
        pass
