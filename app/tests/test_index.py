import unittest
from xml.etree import ElementTree as ET
from datetime import datetime, timezone

from cineplexx_rss.index import build_index_html, build_index_xml, FeedLink


class IndexTests(unittest.TestCase):
    def test_index_contains_feeds(self) -> None:
        feeds = [
            FeedLink(title="Cineplexx", href="cineplexx_rss.xml", kind="cineplexx"),
            FeedLink(title="Telegram — t.me/durov", href="durov.xml", kind="telegram"),
        ]
        now = datetime(2026, 1, 4, 0, 0, 0, tzinfo=timezone.utc)
        html = build_index_html(feeds=feeds, site_title="MyRssHub", last_updated=now)
        xml = build_index_xml(feeds=feeds, site_title="MyRssHub", last_updated=now)
        self.assertIn("cineplexx_rss.xml", html)
        self.assertIn("durov.xml", html)

        root = ET.fromstring(xml)
        items = root.findall("./channel/item")
        self.assertEqual(len(items), 2)

        links = [item.findtext("link") for item in items]
        self.assertIn("cineplexx_rss.xml", links)
        self.assertIn("durov.xml", links)


if __name__ == "__main__":
    unittest.main()
