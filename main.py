import argparse
import asyncio
import glob
import os
import re
import time

from crawlers.local_book_crawler import LocalBookCrawler
from helpers.logger import Logger


def list_txt(directory, recursive:str = False):
    return glob.glob(os.path.join(directory, "**/*.txt" if recursive else "/*.txt"), recursive=True)


async def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-d", "--directory", nargs="?", const=".", type=str, default=".",
                            help="input directory, default current directory")

    arg_parser.add_argument("-lm", "--log-mode", nargs="?", const=1, type=int, default=1,
                            help="log behaviour [0]: disable [1]: to file [2]: to stdout, default[1]")

    arg_parser.add_argument("-lout", "--log-out", nargs="?", const="", type=str, default="",
                            help="redirect out log directory")

    arg_parser.add_argument("-lerr", "--log-err", nargs="?", const="", type=str, default="",
                            help="redirect err log directory")

    arg_parser.add_argument("-r", "--recursive", nargs="?", const=True, type=bool, default=False,
                            help="recursive search")

    arg_parser.add_argument("-host", "--host", nargs="?", const="", type=str, default="novelcabinet.lndo.site",
                            help="remote api host")

    arg_parser.add_argument("-ns", "--namespace", nargs="?", const="wp-json/kbp/v1", type=str, default="wp-json/kbp/v1",
                            help="remote api host")

    arg_parser.add_argument("-s", "--schema", nargs="?", const="https", type=str, default="https",
                            help="http or https")


    args = arg_parser.parse_args()
    input_directory = args.directory

    logger = Logger() if args.log_mode > 0 else None
    logger and logger.register_context("execution")

    if args.log_mode == 1:
        logger and logger.out_redirect(args.log_out)
        logger and logger.err_redirect(args.log_err)

    # time_start = time.time()
    crawler = LocalBookCrawler()
    # 连接
    async with await crawler.setup_updater(schema=args.schema, host=args.host, base_path=args.namespace):
        # ls = [r"G:\PycharmProjects\novelcabinet.importer\sample-novel.txt"]
        ls = list_txt(input_directory, args.recursive)
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
                    match = re.search(r"作者：(.*?)[&.]", curr_file_name)
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

if __name__ == '__main__':
    asyncio.run(main())
