import asyncio
import glob
import io
import os
import re
import hashlib
import time
from typing import Any, Tuple

from crawlers.book_crawler import AbsBookCrawler
from helpers.logger import Logger
from helpers.txt_reader import TxtBookReader


class LocalBookCrawler(AbsBookCrawler):
    def __init__(self):
        super().__init__()
        self.txt_reader: TxtBookReader = TxtBookReader()
        self.txt_book_data: dict = {}

    def __enter__(self) -> "LocalBookCrawler":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def open(self, file_path: str) -> "TxtBookReader":
        return self.txt_reader.open(file_path)

    def close(self) -> None:
        self.txt_reader.close()

    def load_contents(self, title: str = None, author: str = None) -> Any:

        self.txt_book_data = self.txt_reader.scan_file(
            lambda reader: self._load_contents_scanner_handler(reader, title, author))

    async def debug_print(self, brief: bool = True):
        # print(self.txt_book_data['excerpt'])
        basic_info = await self._get_book_basic_info()
        # print(await self._get_contents_info())
        # print(await self._get_contents_info())
        print(basic_info["title"])
        print(basic_info["author"])
        print("===")
        print(basic_info["excerpt"], end='')
        print("===")

        for volume in (await self._get_contents_info())["volumes"]:
            print("volume: " + volume["title"].strip())
            if not brief:
                print(await self._get_one_chapter(volume['srcIdx']))
            else:
                print(volume['srcIdx'])
            for chapter in volume["chapters"]:
                print("chapter: " + chapter["title"].strip())
                if not brief:
                    print(await self._get_one_chapter(chapter['srcIdx']))
                else:
                    print(chapter['srcIdx'])

        # print(self._get_one_chapter(self._get_contents_info()['volumes'][0]['chapters'][0]['srcIdx']))

    @staticmethod
    def _load_contents_scanner_handler(reader: TxtBookReader, title_input, author_input) -> dict:
        def find_first_volume_or_chapter_char(input_str):
            for char in input_str:
                if char == '卷' or char == '章':
                    return char
            return None

        def byte_to_char(byte_len):
            if reader.encoding == 'utf-8':
                return byte_len / 3
            if reader.encoding == 'gb18030' or reader.encoding == 'gb2312':
                return byte_len / 2
            return len

        def must_be_chapter(s_pos, e_pos):
            # 1000 characters
            return byte_to_char(e_pos - s_pos) > 1000

        def maybe_chapter(s_pos, e_pos):
            # 300 characters
            return byte_to_char(e_pos - s_pos) > 300

        def read_content_block(_last_empty: int) -> Tuple[str, str, int, int]:
            """
            如何区分卷、章、错误排版？
            1. 一般情况下，两行空格是卷、章的标志，但是可以看到存在错误排版：
                i. 只空了一行，但是是卷/章
                ii. 空了两行，但其实并不是卷/章

            首先是不是章/卷，有一个比较准确的判断：
                i. 出现了卷/章这种字眼
                ii. 行字符小于50
            这样十有八九是卷，如果这都不是，我自认倒霉

            对于是不是章，可以这么判断：
            如果内容长（大于3000）字节，那么十有八九是章，如果这都不是，我自认倒霉

            2. 需要区分的内容有四类：
                i. 简介
                ii. 卷
                iii. 章
                iv. 其它真的不能归于任何一类的自然段
            我们希望iv越少越好
            为了减少孤儿自然段，考虑将其进行计数，在1.ii的基础上，如果长度短于3000字节，把它归为上一部分中

            """
            _title = ""
            _is_volume = False
            _is_chapter = False
            # 有没有标题先？
            line = reader.peek_line().lower()
            # print("line: " + line)
            if len(line) <= 50:
                # 也许是标题了
                _title = reader.readline()
                _next_line = reader.peek_line()

                # 标题确定，剩下的就是内容了
                s_pos = reader.tell()
                reader.read_continuous_content_lines()
                e_pos = reader.tell()

                # 那下面是可以实锤的情况
                # 1. 小说里字很多的是什么？章节！
                if must_be_chapter(s_pos, e_pos):
                    _is_chapter = True
                else:
                    # 2. 好吧，命中了至少一个，认为必出一狼
                    char = find_first_volume_or_chapter_char(line)
                    if char == '卷':
                        _is_volume = True
                    elif char == '章':
                        _is_chapter = True
                    # 3. 如果下一行是空行（没有内容），就是卷
                    # elif reader.is_line_empty(reader.peek_line()):
                    elif reader.is_line_empty(_next_line):
                        _is_volume = True

                # 捶不了，只能推测了
                if not _is_chapter and not _is_volume:
                    # 上段空行等于1或小于1（初始情况），推测更可能是继承，必须有关键字存在才是卷、章
                    if _last_empty <= 1:
                        # 由于和实锤相同，跳过即可
                        pass
                    # 上段空行大于1，推测更可能是卷、章
                    else:
                        # s_pos = reader.tell()
                        # # print(s_pos)
                        # reader.read_continuous_content_lines()
                        # e_pos = reader.tell()
                        # print(e_pos)
                        # skip_count = reader.read_continuous_empty_lines()
                        if maybe_chapter(s_pos, e_pos):
                            return _title, "chapter", s_pos, e_pos
                        else:
                            return _title, "volume", s_pos, e_pos

            else:
                s_pos = reader.tell()
                # print(s_pos)
                reader.read_continuous_content_lines()
                e_pos = reader.tell()
            # print(e_pos)
            # skip_count = reader.read_continuous_empty_lines()
            # print("chapter" if _is_chapter else "volume" if _is_volume else "inherit")
            # print("123" + reader.get_between_text(s_pos, e_pos))
            # print("1234" + _title)
            # print(f"12345 {s_pos}, {e_pos}")

            return _title, "chapter" if _is_chapter else "volume" if _is_volume else "inherit", s_pos, e_pos

        result = {}

        # 1. [去尾] 去除尾部的宣传
        reader.seek(0, io.SEEK_END)
        content_end_pos = reader.tell()  # 最后有效的结尾位置

        trail = reader.readline_backward()
        if re.search(r"===+", trail):
            [reader.readline_backward() for _ in range(2)]
            content_end_pos = reader.tell()

        # 2. [去头] 去除头部的宣传
        reader.seek(0)
        first_line = reader.readline()
        if re.search(r"===+", first_line):
            [reader.readline() for _ in range(2)]

        # 3. 去除中间可能的空行
        reader.read_continuous_empty_lines()

        # 4. [书名/作者] 一般是书名和作者，可能会同时存在一行，或者分两行，作者名可能缺失，但书名一般有
        first_line = reader.readline()
        # 如果下一行是空行，那么就是同行
        if LocalBookCrawler.empty_line(reader.peek_line()):
            title_read, author_read = LocalBookCrawler.try_split_title_author(first_line)
        # 否则一行是题目，一行是作者
        else:
            title_read = first_line
            author_read = reader.readline()

        title = title_input or title_read
        author = author_input or author_read

        # 4.1 记录
        # todo: 允许外部传入书名、作者，为空时才考虑这个
        # todo: 如果外部和parse的不同步，是不是应该给出warning
        result['title'] = LocalBookCrawler.filter_title(title)
        result['author'] = LocalBookCrawler.filter_author(author)

        # 5. 去除中间可能的空行
        # print(reader.tell())
        reader.read_continuous_empty_lines()
        # print(reader.tell())

        def append_volume(_result, _title, _src_idx=None):
            if _src_idx is None:
                _src_idx = [0, 0]
            if 'volumes' not in _result:
                _result['volumes'] = []
            _result['volumes'].append({
                        "title": _title,
                        "content": "",
                        "srcIdx": _src_idx,
                        "chapters": []
                    })

        def append_chapter(_result, _title, _src_idx):
            if 'volumes' not in _result or len(_result["volumes"]) == 0:
                append_volume(_result, "正文卷")
            _result['volumes'][-1]['chapters'].append({
                "title": _title,
                "srcIdx": _src_idx,
            })

        # 主要内容
        last_empty = 0
        result['volumes'] = []
        result["excerpt"] = ""
        # # [内容简介]
        # title, is_volume, start_pos, end_pos = read_content_block(last_empty)
        # # 开头的：明确包含简介等关键字，或者是短文，认为是简介
        # if re.search(r"(内容|介绍|简介)", title) or (
        #         not title and not maybe_chapter(start_pos, end_pos) and end_pos - start_pos > 0):
        #     result['excerpt'] = title + reader.get_between_text(start_pos, end_pos)
        #     print(result['excerpt'])
        # elif is_volume:
        #     append_volume(result, title, [start_pos, end_pos])
        # else:
        #     append_chapter(result, title, [start_pos, end_pos])
        #
        # last_empty = reader.read_continuous_empty_lines()
        #
        # # 不要作者简介
        # title, is_volume, start_pos, end_pos = read_content_block(last_empty)
        # if re.search(r"(作者)", title) or (
        #         not title and not maybe_chapter(start_pos, end_pos) and end_pos - start_pos > 0):
        #     pass
        # elif is_volume:
        #     append_volume(result, title, [start_pos, end_pos])
        # else:
        #     append_chapter(result, title, [start_pos, end_pos])
        #
        # last_empty = reader.read_continuous_empty_lines()

        cnt: int = 0
        last_type = ""
        while True:
            # print("Loop begin")
            title, this_type, start_pos, end_pos = read_content_block(last_empty)
            end_pos = min(content_end_pos, end_pos)
            last_empty = reader.read_continuous_empty_lines()
            done = False

            if cnt < 2:
                # 不要作者简介
                if re.search(r"(作者)", title):
                    done = True
                # 开头的：明确包含简介等关键字，或者是短文，认为是简介
                elif not result["excerpt"]:
                    if re.search(r"(内容|介绍|简介)", title) or (
                            not title and not maybe_chapter(start_pos, end_pos) and end_pos - start_pos > 0):
                        # print(start_pos)

                        result["excerpt"] += reader.get_between_text(start_pos, end_pos)
                        # print("123" + result["excerpt"])
                        last_type = "excerpt"
                        done = True

            if not done:
                # print("346" + reader.get_between_text(start_pos, end_pos))
                # print(this_type + " " + last_type)
                # inherit
                if this_type == "inherit":
                    if not last_type or last_type is "excerpt":
                        result["excerpt"] += "\n"
                        result["excerpt"] += title + reader.get_between_text(start_pos, end_pos)
                        last_type = "excerpt"
                    elif last_type is "volume":
                        result["volumes"][-1]["srcIdx"][1] = end_pos
                        last_type = "volume"
                    else:
                        result["volumes"][-1]["chapters"][-1]["srcIdx"][1] = end_pos
                        last_type = "chapter"

                elif this_type == "volume":
                    append_volume(result, LocalBookCrawler.strip_empty_space(title), [start_pos, end_pos])
                    last_type = "volume"
                else:
                    append_chapter(result, LocalBookCrawler.strip_empty_space(title), [start_pos, end_pos])
                    last_type = "chapter"

            if last_empty == 0:
                break
            cnt += 1

        return result

    @staticmethod
    def try_split_title_author(line: str) -> Tuple[str, str]:
        """
        try split title and author from one line
        attempts: 1. split by: ”作者：“
                  2. split by: empty string
        :param line:
        :return:
        """
        # 1. strip empty characters
        line = LocalBookCrawler.strip_empty_space(line)

        def try_split_to_ls(separator: str) -> list:
            return list(filter(lambda x: not LocalBookCrawler.empty_line(x),
                        map(LocalBookCrawler.remove_empty_space, line.split(separator))))

        # 2. split by ”作者：“
        ls = try_split_to_ls("作者：")
        if len(ls) >= 2:
            return ls[0], ls[1]

        # 3. split by " "
        ls = try_split_to_ls(" ")
        if len(ls) >= 2:
            return ls[0], ls[1]

        # 4. split by unicode whitespace "　"
        ls = try_split_to_ls("　")
        if len(ls) >= 2:
            return ls[0], ls[1]

        # 5. ok, give up
        return line, ""

    @staticmethod
    def empty_line(line: str) -> bool:
        return len(LocalBookCrawler.remove_empty_space(line)) == 0

    @staticmethod
    def filter_author(author: str):
        return LocalBookCrawler.remove_empty_space(author).replace("作者：", "")

    @staticmethod
    def filter_title(title: str):
        return LocalBookCrawler.remove_empty_space(title).replace("《", "").replace("》", "").replace("书名：", "")

    @staticmethod
    def remove_empty_space(string: str):
        return string.replace("　", "").replace(" ", "").strip()

    @staticmethod
    def strip_empty_space(string: str):
        return string.strip()

    async def _get_book_basic_info(self) -> dict:
        return {
            "title": self.txt_book_data["title"],
            "author": {
                "login": hashlib.md5(self.txt_book_data["author"].encode()).hexdigest(),
                "name": self.txt_book_data["author"]
            },
            "excerpt": self.txt_book_data["excerpt"] if "excerpt" in self.txt_book_data else "",
            "genres": self.txt_book_data["genres"] if "genres" in self.txt_book_data else [],
            "tags": self.txt_book_data["tags"] if "tags" in self.txt_book_data else [],
        }

    async def _get_contents_info(self):
        return self.txt_book_data

    async def _get_one_chapter(self, src_idx: list) -> str:
        if src_idx and len(src_idx) >= 2:
            return self.txt_reader.get_between_text(src_idx[0], src_idx[1])
        return ""

    @staticmethod
    def get_genre_mapping(outer_genres):
        def mp(genre):
            mapping = {"外部传记": "传记", "外部剧情": "剧情", "外部剧情2": "剧情", "外部历史": "历史", "外部恐怖": "恐怖"}
            if genre in mapping:
                return mapping[genre]
            return ""

        return list(filter(lambda x: x != "", map(mp, outer_genres)))


def list_txt(directory):
    return glob.glob(os.path.join(directory, '**/*.txt'), recursive=True)


async def main():
    current_directory = r"C:\Users\15472\Desktop\novels"

    logger = Logger()
    # logger = None
    logger and logger.register_context("execution")

    logger and logger.out_redirect("")
    logger and logger.err_redirect("")

    # time_start = time.time()
    crawler = LocalBookCrawler()
    # 连接
    async with await crawler.setup_updater():
        # ls = [r"G:\PycharmProjects\novelcabinet.importer\sample-novel.txt"]
        ls = list_txt(current_directory)
        # txt一览
        for file_path in ls:
            # 打开一个
            try:
                with crawler.open(file_path):
                    # 开始log
                    curr_file_name = os.path.basename(file_path)
                    logger and logger.write_log("name", curr_file_name, "incremental insert")
                    print(curr_file_name)
                    time_start = time.time()

                    # 尝试提取书名和作者
                    match = re.search(r"《(.*?)》", curr_file_name)
                    title = match.group(1) if match else None
                    match = re.search(r"作者：(.*?)\.", curr_file_name)
                    author = match.group(1) if match else None
                    if author is None and title is None:
                        match = re.match(r"(.*?)[（.]", curr_file_name)
                        title = match.group(1) if match else None
                    # print(title)
                    # print(author)
                    # print("a?")
                    # 插入
                    crawler.load_contents(title, author)
                    # await crawler.debug_print()
                    result = await crawler.incremental_insert(logger=logger)

                    # # 结束log
                    if not result:
                        raise Exception("incremental_insert return false")

                    time_end = time.time()
                    print(f"execution time: {time_end - time_start}")
            except Exception as e:
                logger and logger.write_err_log(f"{os.path.basename(file_path)}: {repr(e)}", "incremental insert")

        logger and logger.write_log("done", "total", "execution")


async def debug_main():
    logger = None
    # time_start = time.time()
    crawler = LocalBookCrawler()
    # 连接
    async with await crawler.setup_updater():
        ls = [r"G:\PycharmProjects\novelcabinet.importer\sample-novel.txt"]
        # ls = [r"C:\Users\15472\Desktop\novels\《从零开始》（校对版全本）作者：雷云风暴.txt"]
        # txt一览
        for file_path in ls:
            # 打开一个
            try:
                with crawler.open(file_path):
                    # 开始log
                    curr_file_name = os.path.basename(file_path)
                    logger and logger.write_log("name", curr_file_name, "incremental insert")
                    print(curr_file_name)
                    time_start = time.time()

                    # 插入
                    crawler.load_contents()
                    await crawler.debug_print(False)

                    time_end = time.time()
                    print(f"execution time: {time_end - time_start}")
            except Exception as e:
                logger and logger.write_err_log(f"{os.path.basename(file_path)}: {repr(e)}", "incremental insert")

        logger and logger.write_log("done", "total", "execution")
# time_end = time.time()
# print(f"execution time: {time_end - time_start}")

if __name__ == '__main__':
    asyncio.run(main())

