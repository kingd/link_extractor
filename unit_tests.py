import unittest
from multiprocessing import Queue

from link_extractor import extract_url_links


TEST_MARKUP = """
<!DOCTYPE html>
<html>
<body>
    <a href="https://www.google.com">google</a>
    <a href="https://www.yahoo.com">google</a>
    <a href="https://www.ebay.com">google</a>
    <a href="https://www.paypal.com">google</a>
</body>
</html>
"""


class TestStringMethods(unittest.TestCase):

    def test_extract_url_links(self):
        links = []
        task = {'url': 'http://example.com', 'markup': TEST_MARKUP}
        queue = Queue(10)
        queue.put(task)
        queue.put(None)
        extract_url_links(queue, links)
        self.assertEqual(len(links), 4)

        queue.put(task)
        queue.put(None)
        extract_url_links(queue, links)
        self.assertEqual(len(links), 8)


if __name__ == '__main__':
    unittest.main()
