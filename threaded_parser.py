import argparse
import os
import pathlib
import queue
import re
import signal
import sys
import threading
import time
import urllib.request
from typing import Optional
from urllib.error import HTTPError, URLError

HABR_MAIN = "https://habr.com"

HABR_ARTICLE_LINKS_REGEXP = re.compile(
    r'(?<=<a href=")[^>]+(?=" class="tm-article-snippet__readmore")')

IMAGE_REGEXP = re.compile(r'(?<=src=")https://habrastorage[^"]+')
ARTICLE_TITLE_REGEXP = re.compile(
    r'(?<='
    r'class="tm-article-snippet__title tm-article-snippet__title_h1"><span>)'
    r'[^<]+(?=</span>)')

WINDOWS_DIRECTORY_INVALID_SYMBOLS = ['<', '>', ':', '"', '\\',
                                     '/', '|', '?', '*', '&nbsp;', '\xa0']


class Article:
    def __init__(self, link, name: str = None):
        self.link = link
        self.images_links = []
        self.name = name


class ArticlesProvider:
    def __init__(self, articles_arr: []):
        self.articles = queue.Queue()
        for article in articles_arr:
            self.articles.put(article)

    def get_article_to_handle(self):
        if self.articles.empty():
            return None
        return self.articles.get()

    def is_finish(self):
        return self.articles.empty()


class GracefulKiller:
    graceful_kill = threading.Event()

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.graceful_kill.set()
        write_log('___Graceful kill___')


def get_articles_links(n):
    links = []
    while n > 0:
        one_page_links = [HABR_MAIN + art_link for art_link in
                          get_items(HABR_MAIN + f'/ru/all/page{n // 20 + 1}',
                                    HABR_ARTICLE_LINKS_REGEXP, min(n, 20))]
        for link in one_page_links:
            links.append(link)
        n -= 20
    return links


def parse_images(link):
    return get_items(link, IMAGE_REGEXP)


def download_image(img_link, out_path):
    urllib.request.urlretrieve(img_link, out_path)


def download_all_images(links, out_dir):
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    for i in range(len(links)):
        download_image(links[i], out_dir + f"/{i}.jpg")


def get_articles_info(links):
    articles = []
    for link in links:
        titles_candidates = get_items(link, ARTICLE_TITLE_REGEXP)
        if not titles_candidates:
            raise Exception(f"title does not exists in: {link}")
        article = Article(link, titles_candidates[0])
        article.images_links = parse_images(link)
        articles.append(article)
    return articles


def save_images(articles, out_path):
    if not os.path.exists(out_path):
        os.mkdir(out_path)
    for art in articles:
        if not art.images_links:
            continue
        directory_name = art.name
        for s in WINDOWS_DIRECTORY_INVALID_SYMBOLS:
            directory_name = directory_name.replace(s, '')
        download_all_images(art.images_links,
                            out_path + '/' + directory_name)


def get_items(link, regexp, n: int = 0):
    content = load_content(link).decode()
    found = re.findall(regexp, content)
    if n:
        return found[0:n]
    return found


def load_content(url: str) -> Optional[bytes]:
    try:
        return urllib.request.urlopen(url, timeout=10).read()
    except (HTTPError, URLError):
        return None


def start_image_loader(provider: ArticlesProvider, killer: GracefulKiller,
                       out_path: pathlib.Path):
    name = threading.current_thread().name
    write_log(name + ' start')
    while not killer.graceful_kill.is_set():
        article_info = provider.get_article_to_handle()
        if not article_info:
            break
        if not article_info.images_links:
            continue
        write_log(
            f'{name} handling {article_info.name}')
        directory_name = article_info.name
        for s in WINDOWS_DIRECTORY_INVALID_SYMBOLS:
            directory_name = directory_name.replace(s, '')
        download_all_images(article_info.images_links,
                            out_path.name + '/' + directory_name)
        write_log(
            f'{name} end handling {article_info.name}')
    write_log(name + ' finish')


def run_scraper(threads: int, articles_count: int,
                out_dir: pathlib.Path) -> None:
    killer = GracefulKiller()
    articles_links = get_articles_links(articles_count)
    provider = ArticlesProvider(get_articles_info(articles_links))
    pool = []
    threads = min(threads, articles_count)
    for i in range(threads):
        thread = threading.Thread(target=start_image_loader,
                                  args=(provider, killer, out_dir))

        pool.append(thread)
        thread.start()

    while not (killer.graceful_kill.is_set() or provider.is_finish()):
        time.sleep(1)

    for th in pool:
        th.join()


def main():
    script_name = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(
        usage=f'{script_name} [ARTICLES_NUMBER] THREAD_NUMBER OUT_DIRECTORY',
        description='Habr parser',
    )
    parser.add_argument(
        '-n', type=int, default=25, help='Number of articles to be processed',
    )
    parser.add_argument(
        'threads', type=int, help='Number of threads to be run',
    )
    parser.add_argument(
        'out_dir', type=pathlib.Path, help='Directory to download habr images',
    )
    args = parser.parse_args()
    if not os.path.exists(args.out_dir):
        os.mkdir(args.out_dir)

    run_scraper(args.threads, args.n, args.out_dir)


log_writing_lock = threading.Lock()


def write_log(message):
    with log_writing_lock:
        print(f'[info] {message}')


if __name__ == '__main__':
    main()
