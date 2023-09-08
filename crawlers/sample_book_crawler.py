import asyncio

from crawlers.book_crawler import AbsBookCrawler


class SampleBookCrawler(AbsBookCrawler):

    async def _get_book_basic_info(self) -> dict:
        """
        - title
        - author
        - excerpt
        - genres
        - tags

        :return:
        """
        return {
            "title": "lorem...5",
            "author": {
                "login": "mkr67n",
                "name": "JOJO"
            },
            "excerpt": "lorem...",
            "genres": ["外部传记", "外部剧情2", "无效标签"],
            "tags": [],
        }

    async def _get_contents_info(self):
        """
        - volumes: 卷
            - title: 名称
            - chapters: 章节
                - title: 名称
                - srcIdx: 获取内容的必要数据

        :return:
        """
        return {
            "volumes": [
                {
                    "title": "aaa",
                    "chapters": [
                        {
                            "title": "new chapter1",
                            "srcIdx": "content of 111"
                        },
                        {
                            "title": "new chapter2",
                            "srcIdx": "content of 222"
                        },
                        {
                            "title": "new chapter3",
                            "srcIdx": "content of 333"
                        },
                    ]
                },
                {
                    "title": "bbb",
                    "chapters": [
                        {
                            "title": "new chapter1",
                            "srcIdx": "content of 111"
                        },
                        {
                            "title": "new chapter2",
                            "srcIdx": "content of 222"
                        },
                    ]
                },
            ]
        }

    async def _get_one_chapter(self, src_idx):
        return src_idx


async def main():
    crawler = SampleBookCrawler()
    mapping = {"外部传记": "传记", "外部剧情": "剧情", "外部剧情2": "剧情", "外部历史": "历史", "外部恐怖": "恐怖"}
    for k, v in mapping.items():
        crawler.add_genres_mapping_rule(k, v)

    async with await crawler.setup_updater():
        await crawler.incremental_insert()
        pass


if __name__ == '__main__':
    asyncio.run(main())

