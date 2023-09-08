import re
from typing import BinaryIO, AnyStr, Callable, Any, Tuple, Union, List


class TxtBookReader:
    """
    utility class for open a file, scan it as binary, and handle it as if str
    """
    def __init__(self):
        self.encoding: str = ''
        self.file: Union[BinaryIO, None] = None
        # self.file_path = file_path

    def __enter__(self) -> "TxtBookReader":
        # self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def open(self, file_path: str) -> "TxtBookReader":
        if self.file is None:
            self.file = open(file_path, 'rb')
        return self

    def close(self):
        if self.file is not None:
            self.file.close()
            self.file = None

    def scan_file(self, scan_handler: Callable[["TxtBookReader"], Any]) -> Any:
        """
        scan this file with a scan handler
        :param scan_handler: a callable where take this reader as the only parameter, return any
        :return: the return value of scan handler
        """
        try:
            self.__init_scan('gb18030')
            return scan_handler(self)
        except UnicodeDecodeError:
            pass

        self.__init_scan('utf-8')
        return scan_handler(self)

    def __init_scan(self, encoding):
        self.encoding = encoding
        self.reset()

    def reset(self) -> None:
        self.file.seek(0)

    def tell(self) -> int:
        return self.file.tell()

    def seek(self, pos: int, whence: int = 0) -> int:
        return self.file.seek(pos, whence)

    def skip_lines(self, count):
        """
        skip n lines
        :param count: positive: forward, negative: backward
        :return:
        """
        if count > 0:
            for _ in range(count):
                self.readline_binary()
        elif count < 0:
            pos = self.file.tell()
            for _ in range(-count):
                while pos > 0 and self.file.read(1) != b"\n":
                    pos -= 1
                    self.file.seek(pos)

    def read_binary(self, byte: int = 1) -> AnyStr:
        return self.file.read(byte)

    def read(self, byte: int = 1) -> str:
        return self.read_binary(byte).decode(self.encoding)

    def readline_binary(self) -> AnyStr:
        return self.file.readline(-1)

    def readline_backward_binary(self) -> AnyStr:
        """
        read this line, move pointer to the end of last line (before this line)

        :return:
        """
        end = self.tell()
        # print(f"begin: end={end}")
        # 如果当前位置是文档末尾，end理所当然地向前移动一个位置
        if self.peek_binary() == b"":
            end -= 1
            self.seek(max(0, end))

        pos = end
        # 如果当前处于换行符，pos理所当然地向前移动一个位置
        if self.peek_binary() == b"\n":
            pos -= 1
            self.seek(pos)
        # pos = end - 1 if self.peek_binary() == b"\n" else end
        # 向前检索第一个换行符
        # print(f"before while: pos={pos}")
        while pos > 0 and self.peek_binary() != b"\n":
            pos -= 1
            # print(f"in while: {pos}")
            self.seek(pos)

        if pos != 0:
            self.read_binary()
        # pos += 1

        line = self.readline_binary()
        # line = self.readline_binary()
        # print(f"exit function {pos} {end}")
        self.seek(max(0, pos))
        return line

    def readline_backward(self) -> str:
        """
        read this line, move pointer to the end of last line (before this line)

        :return:
        """
        return self.readline_backward_binary().decode(self.encoding)

    def readline(self):
        return self.readline_binary().decode(self.encoding)

    def get_between_text(self, start_pos, end_pos):
        pos = self.tell()
        self.seek(start_pos)
        # content = self.read(end_pos - start_pos).decode(self.encoding)
        content = self.read(end_pos - start_pos)

        self.seek(pos)
        return content

    def peek_line_binary(self) -> AnyStr:
        pos: int = self.tell()
        line_bin = self.readline_binary()
        self.seek(pos)
        return line_bin

    def peek_line(self) -> str:
        return self.peek_line_binary().decode(self.encoding)

    def peek_binary(self) -> AnyStr:
        pos: int = self.tell()
        b = self.read_binary(1)
        self.seek(pos)
        return b

    @staticmethod
    def is_line_empty(line: AnyStr) -> bool:
        """
        check if line only contain white space characters

        :return:
        """
        return not line.strip()

    def read_continuous_empty_lines(self) -> int:
        """
        scan all continuous empty lines

        :return: line count
        """
        line_count: int = 0
        pos: int
        while True:
            pos = self.tell()
            line = self.readline()

            # print(line)

            if not line or not self.is_line_empty(line):
                # print(f"read continuous: {not line} or {not self.is_line_empty(line)}")
                break
            else:
                # print(f"peek: " + peek(file), end='')
                line_count += 1

        self.seek(pos)
        return line_count

    def read_continuous_content_lines(self):
        """
        scan all continues not empty lines

        :return:
        """
        pos: int
        while True:
            pos = self.tell()
            line = self.readline()
            # print(line.decode(ENCODING).strip())

            if not line or self.is_line_empty(line):
                break
        self.seek(pos)

    def read_until_next_block(self, mxm_empty: int = 1):
        """
        (not that utility) read the whole content block

        :param mxm_empty: accept at most x empty lines between
        :return:
        """
        start_pos = self.tell()
        while True:
            # 读连续行
            self.read_continuous_content_lines()
            # 记录末尾（不包含空行）
            end_pos = self.tell()
            # 读空行
            skip_count = self.read_continuous_empty_lines()
            # 到头了，或者间隔大于mxm_empty
            if skip_count == 0 or skip_count > mxm_empty:
                break

        return start_pos, end_pos

    def read_volume_or_chapter(self, mxm_empty: int = 1) -> Tuple[str, bool, int, int]:
        """
        (not that utility) read one block.
        one block is volume if it only contains title but no content.

        :param mxm_empty: accept at most x empty lines between
        :return: tuple: title, is_volume, start_pos in file, end_pos in file
        """
        title = self.readline()
        # 如果下一行是空行，就是卷
        if self.is_line_empty(self.peek_line()):
            is_volume = True
        else:
            is_volume = False

        start_pos, end_pos = self.read_until_next_block(mxm_empty)

        return title, is_volume, start_pos, end_pos


if __name__ == '__main__':
    def main():
        # with TxtBookReader(r"..\sample-novel.txt") as reader:
        with TxtBookReader(r"C:\Users\15472\Desktop\《从零开始》（校对版全本）作者：雷云风暴.txt") as reader:
            scan_result = reader.scan_file(test_scan_handler)

            print(scan_result['excerpt'])
            # print(scan_result)

            # for volume in scan_result['volumes']:
            #     for chapter in volume['chapters']:
            #         print(reader.get_between_text(chapter['srcIdx'][0], chapter['srcIdx'][1]))

        return


    def test_scan_handler(reader: TxtBookReader):
        result = {}

        # 头？题目？
        title = reader.readline()
        if re.match(r"===+", title):
            [reader.readline() for _ in range(2)]
            title = reader.readline()

        # 作者
        author = reader.readline()

        result['title'] = title.strip()
        result['author'] = author.strip()

        reader.read_continuous_empty_lines()

        # 内容简介
        if re.search(r"(内容|介绍|简介)", reader.peek_line()):
            start_pos, end_pos = reader.read_until_next_block()
            result['excerpt'] = reader.get_between_text(start_pos, end_pos)

        result['volumes'] = []
        while True:
            title, is_volume, start_pos, end_pos = reader.read_volume_or_chapter()
            if not title.strip() and start_pos == end_pos:
                break
            else:
                if is_volume:
                    result['volumes'].append({
                        'title': title,
                        'chapters': []
                    })
                else:
                    result['volumes'][-1]['chapters'].append({
                        'title': title,
                        'srcIdx': [start_pos, end_pos]
                    })

        return result

    main()
