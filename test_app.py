import unittest
from unittest import mock
import sqlite3
import datetime as dt

import app
import github_actions


class MonitorTests(unittest.TestCase):
    def test_static_report_builds_without_server(self):
        sample = {
            "refresh": {}, "sites": [{"id": "example", "name": "Example", "home_url": "https://example.com", "category": "智库", "baseline_count": 1, "channel_count": 1, "error_count": 0, "last_ok_at": "2026-01-01T00:00:00+00:00"}], "channels": [],
            "reports": [{"id": 1, "run_id": "run", "site_id": "example", "url": "https://example.com/story", "title": "Story", "title_zh": "报道", "site_name": "Example", "category": "智库", "created_at": "2026-01-01T00:00:30+00:00"}],
            "runs": [{"id": "run", "category": "智库", "started_at": "2026-01-01T00:00:00+00:00", "finished_at": "2026-01-01T00:01:00+00:00", "new_count": 0, "ok_count": 1, "error_count": 0, "status": "done"}],
        }
        with mock.patch.object(github_actions.app, "state_payload", return_value=sample), mock.patch.object(github_actions, "PUBLIC", app.ROOT / "test-public"):
            github_actions.build_static_report()
            output = (app.ROOT / "test-public" / "index.html").read_text(encoding="utf-8")
            self.assertIn("信息链接监控报告", output)
            self.assertIn("前往 Actions 手动刷新", output)
            self.assertIn('<a href="${esc(x.url)}" target="_blank" rel="noopener noreferrer">${esc(x.url)}</a>', output)
            self.assertIn('id="sitePicker"', output)
            self.assertIn('selectedSites.has(x.site_id)', output)
            self.assertIn("siteCategories=['新闻','智库','央行']", output)
            self.assertIn('data-category-all', output)
            self.assertIn('syncCategorySelectors', output)
            (app.ROOT / "test-public" / "index.html").unlink()
            (app.ROOT / "test-public").rmdir()

    def test_site_categories_are_complete_and_unique(self):
        ids = [site_id for site_id, _, _ in app.SITES]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(set(app.SITE_CATEGORIES), set(ids))
        self.assertEqual({app.SITE_CATEGORIES[site_id] for site_id in ids}, set(app.CATEGORIES))
        self.assertEqual(len(app.NEWS_SITES), 36)
        self.assertEqual(len(app.THINK_TANK_SITES), 36)
        self.assertEqual(len(app.CENTRAL_BANK_SITES), 13)
        self.assertTrue({"scmp-business", "morningstar", "tradingeconomics", "straitstimes", "tradingview"}.issubset(app.REMOVED_SITE_IDS))

    def test_removed_sites_are_cleaned_without_losing_deduplication(self):
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        db.executescript("""
            CREATE TABLE sites(id TEXT PRIMARY KEY);
            CREATE TABLE channels(id INTEGER PRIMARY KEY,site_id TEXT);
            CREATE TABLE seen(channel_id INTEGER,url_hash BLOB,first_seen_at TEXT);
            CREATE TABLE reports(site_id TEXT,url TEXT,created_at TEXT);
            CREATE TABLE reported_fingerprints(url_hash BLOB PRIMARY KEY,first_reported_at TEXT);
            INSERT INTO sites VALUES('ap');
            INSERT INTO channels VALUES(1,'ap');
            INSERT INTO seen VALUES(1,X'01','2026-08-18T00:00:00+00:00');
            INSERT INTO reports VALUES('ap','https://apnews.com/article/example','2026-08-18T00:00:00+00:00');
        """)
        self.assertEqual(app.remove_configured_sites(db), 1)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM sites").fetchone()[0], 0)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM channels").fetchone()[0], 0)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM seen").fetchone()[0], 0)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM reports").fetchone()[0], 0)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM reported_fingerprints").fetchone()[0], 1)

    def test_research_categories_allow_publication_paths(self):
        url = "https://example.com/category/reports"
        self.assertFalse(app.likely_page(url, "https://example.com/", "新闻"))
        self.assertTrue(app.likely_page(url, "https://example.com/", "智库"))
        self.assertTrue(app.likely_page(url, "https://example.com/", "央行"))

    def test_canonical_url_removes_tracking_and_fragment(self):
        self.assertEqual(app.canonical_url("HTTPS://Example.com/a/?utm_source=x&b=2#top"), "https://example.com/a?b=2")

    def test_feed_parser(self):
        body = b'''<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Hello</title><link href="/story/1" /></entry></feed>'''
        self.assertEqual(app.parse_feed(body, "https://example.com/feed"), {"https://example.com/story/1": "Hello"})

    def test_feed_parser_rejects_stale_dated_items(self):
        body = b'''<rss><channel><item><title>Old</title><link>https://example.com/old</link><pubDate>Fri, 14 Aug 2020 16:12:40 +0000</pubDate></item></channel></rss>'''
        now = dt.datetime(2026, 8, 16, 12, 0, tzinfo=dt.timezone.utc)
        self.assertEqual(app.parse_feed(body, "https://example.com/feed", now), {})

    def test_publication_window_covers_both_timezones(self):
        now = dt.datetime(2026, 8, 16, 3, 0, tzinfo=dt.timezone.utc)
        self.assertTrue(app.publication_is_current(dt.datetime(2026, 8, 15, 5, 0, tzinfo=dt.timezone.utc), now))
        self.assertTrue(app.publication_is_current(dt.datetime(2026, 8, 15, 16, 0, tzinfo=dt.timezone.utc), now))
        self.assertFalse(app.publication_is_current(dt.datetime(2026, 8, 14, 20, 0, tzinfo=dt.timezone.utc), now))

    def test_title_parser_prefers_open_graph_title(self):
        parser = app.TitleParser()
        parser.feed('<html><head><title>Fallback</title><meta property="og:title" content="Story title"></head></html>')
        self.assertEqual(parser.value(), "Story title")

    def test_chinese_title_needs_no_remote_translation(self):
        self.assertEqual(app.translate_title("中文标题"), ("中文标题", ""))
        self.assertEqual(app.translate_title_with_language("中文标题"), ("中文标题", "", "zh"))

    def test_title_from_article_url(self):
        url = "https://example.com/article/apple-releases-new-iphone.html?ref=home"
        self.assertEqual(app.title_from_url(url), "Apple releases new iphone")

    def test_title_from_url_rejects_numeric_id(self):
        self.assertEqual(app.title_from_url("https://example.com/news/123456"), "")

    def test_clean_title_removes_ids_and_random_suffixes(self):
        self.assertEqual(app.clean_title("6031907 blanche says fund dead", "https://example.com/blanche-says-fund-dead"), "blanche says fund dead")
        self.assertEqual(app.clean_title("Trump urges daylight savings time g9iia66gK5phMDgWFEnc", "https://example.com/story"), "Trump urges daylight savings time")
        self.assertEqual(app.clean_title("Europe gas storage crunch 210000091", "https://example.com/story"), "Europe gas storage crunch")

    def test_local_language_fallback_rejects_non_english(self):
        self.assertTrue(app.likely_non_english_title("Policía arresta a sospechoso tras incidente"))
        self.assertTrue(app.likely_non_english_title("俄羅斯舉行全國比賽"))
        self.assertFalse(app.likely_non_english_title("Police arrest a suspect after a security incident"))

    def test_historical_full_date_url_is_hidden(self):
        today = dt.date(2026, 8, 16)
        self.assertTrue(app.url_has_non_current_date("https://example.com/2026/08/15/story", today))
        self.assertTrue(app.url_has_non_current_date("https://example.com/story-20260815", today))

    def test_current_and_undated_urls_are_visible(self):
        today = dt.date(2026, 8, 16)
        self.assertFalse(app.url_has_non_current_date("https://example.com/2026-08-16/story", today))
        self.assertFalse(app.url_has_non_current_date("https://example.com/latest/story", today))

    def test_historical_month_url_is_hidden(self):
        today = dt.date(2026, 8, 16)
        self.assertTrue(app.url_has_non_current_date("https://example.com/archive/2026-07/story", today))

    def test_beijing_and_eastern_dates_are_both_current(self):
        now = dt.datetime(2026, 8, 16, 3, 0, tzinfo=dt.timezone.utc)
        allowed = app.current_news_dates(now)
        self.assertEqual(allowed, {dt.date(2026, 8, 16), dt.date(2026, 8, 15)})
        self.assertFalse(app.url_has_non_current_date("https://example.com/2026/08/16/story", allowed))
        self.assertFalse(app.url_has_non_current_date("https://example.com/2026/08/15/story", allowed))
        self.assertTrue(app.url_has_non_current_date("https://example.com/2026/08/14/story", allowed))

    def test_yahoo_quote_pages_are_ignored(self):
        self.assertTrue(app.is_ignored_content_url("https://finance.yahoo.com/quote/PNC"))
        self.assertTrue(app.is_ignored_content_url("https://finance.yahoo.com/quote/HG%3DF/history"))
        self.assertFalse(app.is_ignored_content_url("https://finance.yahoo.com/markets/stocks/articles/story.html"))

    def test_biggo_quote_pages_are_ignored(self):
        self.assertTrue(app.is_ignored_content_url("https://finance.biggo.com/quote/NET"))
        self.assertTrue(app.is_ignored_content_url("https://finance.biggo.com/quote/BAC-PK"))
        self.assertFalse(app.is_ignored_content_url("https://finance.biggo.com/topics/Latest"))

    def test_beijing_and_eastern_dates_are_both_current(self):
        now = dt.datetime(2026, 8, 16, 3, 0, tzinfo=dt.timezone.utc)
        allowed = app.current_news_dates(now)
        self.assertEqual(allowed, {dt.date(2026, 8, 16), dt.date(2026, 8, 15)})
        self.assertFalse(app.url_has_non_current_date("https://example.com/2026/08/16/story", allowed))
        self.assertFalse(app.url_has_non_current_date("https://example.com/2026/08/15/story", allowed))
        self.assertTrue(app.url_has_non_current_date("https://example.com/2026/08/14/story", allowed))

    def test_sitemap_index_keeps_children_separate(self):
        body = b'''<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><sitemap><loc>https://example.com/a.xml</loc></sitemap></sitemapindex>'''
        pages, children = app.parse_sitemap(body, "https://example.com/sitemap.xml")
        self.assertEqual(pages, {})
        self.assertEqual(children, {"https://example.com/a.xml"})

    def test_sitemap_pages(self):
        body = b'''<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>https://example.com/a?utm_source=x</loc></url></urlset>'''
        pages, children = app.parse_sitemap(body, "https://example.com/sitemap.xml")
        self.assertEqual(pages, {"https://example.com/a": ""})
        self.assertEqual(children, set())

    def test_sitemap_score_prefers_current_news(self):
        year = app.dt.datetime.now(app.dt.timezone.utc).year
        current = f"https://example.com/sitemap/news/{year}/index.xml"
        archive = "https://example.com/sitemap/video/2018/index.xml"
        self.assertGreater(app.sitemap_score(current), app.sitemap_score(archive))

    def test_sitemap_score_prefers_newest_reuters_page(self):
        newest = "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml"
        older = "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=6200"
        self.assertGreater(app.sitemap_score(newest), app.sitemap_score(older))

    def test_prune_sitemaps_keeps_explicit_and_enforces_limit(self):
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        db.executescript("""
            CREATE TABLE sites(id TEXT PRIMARY KEY);
            CREATE TABLE channels(id INTEGER PRIMARY KEY,site_id TEXT,kind TEXT,url TEXT,is_explicit INTEGER);
            CREATE TABLE seen(channel_id INTEGER,url TEXT);
            INSERT INTO sites VALUES('site');
        """)
        for index in range(12):
            db.execute("INSERT INTO channels(site_id,kind,url,is_explicit) VALUES('site','sitemap',?,?)", (f"https://example.com/{2010 + index}.xml", int(index == 0)))
        app.prune_sitemaps(db)
        rows = db.execute("SELECT is_explicit FROM channels").fetchall()
        self.assertEqual(len(rows), app.MAX_SITEMAPS_PER_SITE)
        self.assertIn(1, [row[0] for row in rows])

    def test_seen_migration_replaces_urls_with_hashes(self):
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        db.execute("CREATE TABLE seen(channel_id INTEGER,url TEXT,title TEXT,first_seen_at TEXT)")
        db.execute("INSERT INTO seen VALUES(1,'https://example.com/story','Story','2026-01-01T00:00:00+00:00')")
        app.migrate_seen_to_fingerprints(db)
        columns = {row[1] for row in db.execute("PRAGMA table_info(seen)")}
        self.assertEqual(columns, {"channel_id", "url_hash", "first_seen_at"})
        self.assertEqual(db.execute("SELECT url_hash FROM seen").fetchone()[0], app.url_fingerprint("https://example.com/story"))

    def test_cleanup_history_keeps_only_three_days(self):
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        db.executescript("""
          CREATE TABLE reports(url TEXT,created_at TEXT);
          CREATE TABLE runs(started_at TEXT,finished_at TEXT);
          CREATE TABLE reported_fingerprints(url_hash BLOB PRIMARY KEY,first_reported_at TEXT);
        """)
        now = dt.datetime.now(dt.timezone.utc)
        old = (now - dt.timedelta(days=4)).isoformat(timespec="seconds")
        fresh = now.isoformat(timespec="seconds")
        db.executemany("INSERT INTO reports VALUES(?,?)", [("https://example.com/old", old), ("https://example.com/new", fresh)])
        db.executemany("INSERT INTO runs VALUES(?,?)", [(old, old), (fresh, fresh)])
        app.cleanup_history(db)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM reports").fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM runs").fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM reported_fingerprints").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
