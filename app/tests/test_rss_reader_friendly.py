import json
import tempfile
import unittest
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path

from cineplexx_rss.models import Movie, Session
from cineplexx_rss.rss import build_rss_xml
from cineplexx_rss.state import load_state, save_state


class RssReaderFriendlyTests(unittest.TestCase):
    def test_snapshot_migration(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = tempfile.NamedTemporaryFile(dir=tmpdir, delete=False)
            path.write(
                json.dumps(
                    {"snapshot": {"https://x": "Title"}, "events": []},
                    ensure_ascii=False,
                ).encode("utf-8")
            )
            path.close()

            state = load_state(Path(path.name))
            self.assertIn("https://x", state.snapshot)
            meta = state.snapshot["https://x"]
            self.assertEqual(meta["title"], "Title")
            self.assertTrue(meta["first_seen"])
            self.assertTrue(meta["last_seen"])

            save_state(Path(path.name), state)
            saved = json.loads(Path(path.name).read_text("utf-8"))
            self.assertIsInstance(saved["snapshot"]["https://x"], dict)

    def test_pubdate_stable_for_current_items(self) -> None:
        first_seen = "2026-01-04T01:23:45+00:00"
        snapshot_meta = {"https://cineplexx.me/film/Test": {"title": "Test", "first_seen": first_seen, "last_seen": first_seen}}
        movie = Movie(title="Test", url="https://cineplexx.me/film/Test", description="Desc", sessions=[])
        now1 = datetime(2026, 1, 5, 10, 0, 0, tzinfo=timezone.utc)
        now2 = datetime(2026, 1, 6, 10, 0, 0, tzinfo=timezone.utc)
        rss1 = build_rss_xml(
            title="Feed",
            link="https://cineplexx.me",
            description="Desc",
            now=now1,
            events=[],
            events_limit=0,
            current_items=[movie],
            snapshot_meta=snapshot_meta,
        )
        rss2 = build_rss_xml(
            title="Feed",
            link="https://cineplexx.me",
            description="Desc",
            now=now2,
            events=[],
            events_limit=0,
            current_items=[movie],
            snapshot_meta=snapshot_meta,
        )
        expected_pub = format_datetime(datetime.fromisoformat(first_seen))
        self.assertIn(expected_pub, rss1)
        self.assertIn(expected_pub, rss2)

    def test_content_encoded_contains_sessions_list(self) -> None:
        sessions = [
            Session(
                date="2026-01-05",
                time="15:30",
                hall="Sala 2",
                info="2D, SINH",
                session_id="1",
                cinema_name="CINEPLEXX PODGORICA",
                purchase_url="https://cineplexx.me/buy/1",
            )
        ]
        movie = Movie(title="Test", url="https://cineplexx.me/film/Test", description="Desc", sessions=sessions)
        snapshot_meta = {"https://cineplexx.me/film/Test": {"title": "Test", "first_seen": "2026-01-04T01:23:45+00:00", "last_seen": "2026-01-04T01:23:45+00:00"}}
        rss = build_rss_xml(
            title="Feed",
            link="https://cineplexx.me",
            description="Desc",
            now=datetime.now(timezone.utc),
            events=[],
            events_limit=0,
            current_items=[movie],
            snapshot_meta=snapshot_meta,
        )
        self.assertIn('xmlns:content="http://purl.org/rss/1.0/modules/content/"', rss)
        self.assertIn("<content:encoded><![CDATA[", rss)
        self.assertIn("<ul>", rss)
        self.assertIn("<li>", rss)


if __name__ == "__main__":
    unittest.main()
