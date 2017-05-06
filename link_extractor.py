#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Extracts links from a list of urls using Producer/Consumer pattern.
"""
import argparse
import os
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Manager

from scrapy.selector import Selector

def fetch_url_markup(url):
    req = Request(url)
    try:
        response = urlopen(req)
        html = response.read()
        task = {'url': url, 'markup': html}
        return task
    except HTTPError as e:
        print('HTTP error: %s %s' % (e.code, e.reason))
    except URLError as e:
        print('URL error: %s' % e.reason)
    except Exception as e:
        print('Unknown error: %s' % e)


def fetch_url_markups(markup_queue, urls, max_threads=1):
    """Fetches markup from `urls` and stores it into `markup_queue`."""
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        results = executor.map(fetch_url_markup, urls)
        for i, task in enumerate(results, 1):
            if task:
                markup_queue.put(task)
                print('Produced %s: %s' % (i, task['url']))
    markup_queue.put(None)


def extract_url_links(markup_queue, url_links):
    """
    Extracts links from markups stored in `markup_queue` and stores them to
    `url_links`.
    """
    count = 1
    while True:
        task = markup_queue.get()
        if task is None:
            break
        try:
            links = Selector(text=task['markup']).xpath('//a/@href').extract()
            url_links.extend(links)
            print('Consumed %s: %s' % (count, task['url']))
            count += 1
        except Exception as e:
            print('Failed to consume url links %s' % e)


class LinkExtractor(object):
    """
    Fetches markup from `urls` and extraxts links from them using simple
    Producer/Consumer pattern. Max size of the underlying markup_queue can be set with `size`.
    """
    def __init__(self, urls, size=10, max_threads=10):
        self.urls = urls
        self.manager = Manager()
        self.markups = self.manager.Queue(size)
        self.url_links = self.manager.list()
        self.max_threads = max_threads

    def run(self):
        producer = Process(target=fetch_url_markups, args=(self.markups,
                                                           self.urls,
                                                           self.max_threads))
        consumer = Process(target=extract_url_links, args=(self.markups, self.url_links))
        producer.start()
        consumer.start()
        consumer.join()
        return self.url_links


def get_urls_from_file(path):
    if not path or not os.path.isfile(path):
        raise Exception('Path %s is not a valid local path' % path)
    with open(path, 'r') as f:
        return f.read().split()


def parse_args():
    description = """
        Extract urls from an `--url` or list of urls in `--infile`.
        If `--outfile` is not specified stdout is used.
    """
    parser = argparse.ArgumentParser(description=description)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-i', '--infile', help='Input file')
    group.add_argument('-u', '--url', help='Input url')
    parser.add_argument('-o', '--outfile', help='Output file')
    parser.add_argument(
        '-t', '--maxthreads', type=int, default=1,
        help='Number of threads for fetching url markup (default: 1)')
    parser.add_argument(
        '-s', '--qsize', type=int, default=10,
        help='Size of the markup queue (default: 10)')
    return parser.parse_args()


def main():
    args = parse_args()
    if args.url:
        urls = [args.url]
    else:
        urls = get_urls_from_file(args.infile)

    le = LinkExtractor(urls=urls, size=args.qsize, max_threads=args.maxthreads)
    links = le.run()

    if args.outfile:
        with open(args.outfile, 'w+') as f:
            f.write('\n'.join(links))
    else:
        print('\n'.join(links))


if __name__ == '__main__':
    main()
