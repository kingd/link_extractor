#!/usr/bin/env python

import argparse
import os
import random
import sys
import time
import urllib

from multiprocessing import Process, Manager

import requests
from scrapy.selector import Selector


def fetch_url_markups(markup_queue, urls):
    """Fetches markup from `urls` and stores it into `markup_queue`."""
    for url in urls:
        try:
            r = requests.get(url)
            if r.status_code == 200:
                task = {'url': url, 'markup': r.text}
                markup_queue.put(task)
            else:
                print('Response is not valid for %s: %s.' % (url, status_code))
        except Exception as e:
            print('Error while extracting markup from %s: %s.' % (url, e))
    markup_queue.put(None)


def extract_url_links(markup_queue, url_links):
    """
    Extracts links from markups stored in `markup_queue` and stores them to
    `url_links`.
    """
    while True:
        task = markup_queue.get()
        if task is None:
            break
        try:
            links = Selector(text=task['markup']).xpath('//a/@href').extract()
            url_links.extend(links)
        except Exception as e:
            print('Failed to consume url links %s' % e)


class LinkExtractor(object):
    """
    Fetches markup from `urls` and extraxts links from them using simple
    Producer/Consumer pattern. Max size of the underlying markup_queue can be set with `size`.
    """
    def __init__(self, urls, size=10):
        self.urls = urls
        self.manager = Manager()
        self.markups = self.manager.Queue(size)
        self.url_links = self.manager.list()

    def run(self):
        producer = Process(target=fetch_url_markups, args=(self.markups, self.urls))
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
    return parser.parse_args()


def main():
    args = parse_args()
    if args.url:
        urls = [args.url]
    else:
        urls = get_urls_from_file(args.infile)

    links = LinkExtractor(urls=urls).run()

    if args.outfile:
        with open(args.outfile, 'w+') as f:
            f.write('\n'.join(links))
    else:
        print('\n'.join(links))


if __name__ == '__main__':
    main()
