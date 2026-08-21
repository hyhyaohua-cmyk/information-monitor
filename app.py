#!/usr/bin/env python3
"""Local news-link monitor with per-channel baselines."""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import email.utils
import gzip
import hashlib
import html
import html.entities
import json
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "news_monitor.sqlite3"
HOST = "127.0.0.1"
PORT = 8765
MAX_WORKERS = 12
TIMEOUT = 7
MAX_BODY = 12 * 1024 * 1024
MAX_ITEMS_PER_CHANNEL = 10_000
MAX_SITEMAPS_PER_SITE = 8
MAX_SITEMAP_CHILDREN = 6
MAX_SITEMAP_DEPTH = 2
MAX_ENRICH_PER_PASS = 200
TITLE_FETCH_LIMIT = 2 * 1024 * 1024
RETENTION_DAYS = 3
USER_AGENT = "Mozilla/5.0 (compatible; LocalNewsMonitor/1.0; +http://localhost)"

NEWS_SITES = [
    ("bloomberg", "彭博", "https://www.bloomberg.com/latest"),
    ("wsj", "华尔街日报", "https://www.wsj.com/news/latest-headlines"),
    ("marketwatch", "MarketWatch", "https://www.marketwatch.com/latest-news"),
    ("reuters", "路透社", "https://www.reuters.com/"),
    ("cnbc", "CNBC", "https://www.cnbc.com/latest/"),
    ("ft", "金融时报", "https://www.ft.com/news-feed"),
    ("barrons", "Barron's", "https://www.barrons.com/topics/markets?mod=BOL_TOPNAV"),
    ("businessinsider", "Business Insider", "https://www.businessinsider.com/latest"),
    ("biggo", "BigGo Finance", "https://finance.biggo.com/topics/Latest"),
    ("yahoo-finance", "雅虎财经", "https://finance.yahoo.com/topic/latest-news/"),
    ("zerohedge", "ZeroHedge", "https://www.zerohedge.com/"),
    ("fortune", "Fortune", "https://fortune.com/the-latest/"),
    ("investing", "Investing.com", "https://www.investing.com/news/latest-news"),
    ("investinglive", "InvestingLive", "https://investinglive.com/live-feed"),
    ("aljazeera", "Al Jazeera", "https://www.aljazeera.com/news/"),
    ("nikkei", "Nikkei Asia", "https://asia.nikkei.com/Latestheadlines"),
    ("kitco", "Kitco", "https://www.kitco.com/news/digest#latest"),
    ("oilprice", "OilPrice.com", "https://oilprice.com/Latest-Energy-News/World-News/"),
    ("wired", "WIRED", "https://www.wired.com/"),
    ("techcrunch", "TechCrunch", "https://techcrunch.com/latest/"),
    ("foreignpolicy", "Foreign Policy", "https://foreignpolicy.com/category/latest/"),
    ("politico", "POLITICO", "https://www.politico.com/politics"),
    ("nyt", "纽约时报", "https://www.nytimes.com/"),
    ("washingtonpost", "华盛顿邮报", "https://www.washingtonpost.com/latest-headlines/"),
    ("nbc", "NBC News", "https://www.nbcnews.com/"),
    ("cnn", "CNN", "https://edition.cnn.com/"),
    ("investopedia", "Investopedia", "https://www.investopedia.com/"),
    ("semafor", "Semafor", "https://www.semafor.com/"),
    ("thehill", "The Hill", "https://thehill.com/"),
    ("financialpost", "Financial Post", "https://financialpost.com/category/news/"),
    ("axios-markets", "Axios 财经与市场", "https://www.axios.com/economy/economy-finance-markets"),
    ("sp-global", "标普全球", "https://www.spglobal.com/market-intelligence/en/news-insights"),
    ("fxstreet", "FXStreet", "https://www.fxstreet.com/news/feed"),
    ("sec", "美国证交会", "https://www.sec.gov/newsroom/press-releases"),
    ("eia", "美国能源信息署", "https://www.eia.gov/todayinenergy/"),
]

THINK_TANK_SITES = [
    ("rand", "兰德公司", "https://www.rand.org/pubs.html?pub-date=20200302%3A&page=5"),
    ("brookings", "布鲁金斯协会", "https://www.brookings.edu/"),
    ("boell", "德国海因里希·伯尔基金会", "https://www.boell.de/en/publications"),
    ("prio", "奥斯陆和平研究所", "https://www.prio.org/Publications/"),
    ("iiss", "英国国际战略研究所", "https://www.iiss.org/"),
    ("fpri", "美国外交政策研究所", "https://www.fpri.org/publications/special-reports/"),
    ("uschamber", "美国商会", "https://www.uschamber.com/"),
    ("itif", "信息科技和创新基金", "https://itif.org/"),
    ("jiia", "日本国际问题研究所", "https://www.jiia.or.jp/en/"),
    ("piie", "彼得森国际经济研究所", "https://www.piie.com/"),
    ("cfr", "外交关系委员会", "https://www.cfr.org/"),
    ("wilson-center", "威尔逊国际学者中心", "https://www.wilsoncenter.org/"),
    ("cato", "加图研究所", "https://www.cato.org/"),
    ("orf", "观察家研究基金会", "https://www.orfonline.org/research/"),
    ("aei-home", "美国企业研究所（首页）", "http://www.aei.org/"),
    ("heritage", "美国传统基金会", "https://www.heritage.org/"),
    ("ifo", "德国 Ifo 研究所", "https://www.ifo.de/"),
    ("swp-berlin", "德国国际与安全事务研究所", "https://www.swp-berlin.org/en/"),
    ("adbi", "亚洲开发银行研究院", "https://www.adb.org/adbi/publications"),
    ("cicir", "中国现代国际关系研究院", "http://www.cicir.ac.cn/NEW/index.html"),
    ("cigi", "加拿大国际治理创新中心", "https://www.cigionline.org/publications"),
    ("kiep", "韩国对外经济政策研究院", "https://www.kiep.go.kr/gallery.es?mid=a20301000000&bid=0007"),
    ("imemo", "俄罗斯世界经济与国际关系研究所", "https://www.imemo.ru/en/publications/list/last?last"),
    ("ispi", "意大利国际政治研究所", "https://www.ispionline.it/en/pubblicazioni"),
    ("ceps", "欧洲政策研究中心", "https://www.ceps.eu/"),
    ("nber-trade-war-paper", "NBER：2018年贸易战论文", "https://www.nber.org/papers/w25672"),
    ("nber", "美国国家经济研究局", "https://www.nber.org/"),
    ("ifri", "法国国际关系研究所", "https://www.ifri.org/"),
    ("taiwan-sig", "台湾新社会智库", "http://www.taiwansig.tw/"),
    ("cnas", "新美国安全中心", "https://www.cnas.org/reports"),
    ("atlantic-council", "大西洋理事会", "https://www.atlanticcouncil.org/in-depth-research-reports/"),
    ("chatham-house", "查塔姆研究所", "https://www.chathamhouse.org/regions/europe"),
    ("csis", "战略与国际研究中心", "https://www.csis.org/analysis"),
    ("fabian-society", "费边社", "https://fabians.org.uk/publications/"),
    ("aei-research", "美国企业研究所（研究产品）", "https://www.aei.org/research-products/"),
    ("elcano", "埃尔卡诺皇家研究所", "https://www.realinstitutoelcano.org/en/"),
]

CENTRAL_BANK_SITES = [
    ("federal-reserve", "美联储", "https://www.federalreserve.gov/"),
    ("new-york-fed", "纽约联储", "https://www.newyorkfed.org/"),
    ("boston-fed", "波士顿联储", "https://www.bostonfed.org/"),
    ("chicago-fed", "芝加哥联储", "https://www.chicagofed.org/"),
    ("san-francisco-fed", "旧金山联储", "https://www.frbsf.org/"),
    ("philadelphia-fed", "费城联储", "https://www.philadelphiafed.org/"),
    ("richmond-fed", "里士满联储", "https://www.richmondfed.org/"),
    ("atlanta-fed", "亚特兰大联储", "https://www.atlantafed.org/"),
    ("cleveland-fed", "克利夫兰联储", "https://www.clevelandfed.org/"),
    ("dallas-fed", "达拉斯联储", "https://www.dallasfed.org/"),
    ("st-louis-fed", "圣路易斯联储", "https://www.stlouisfed.org/"),
    ("kansas-city-fed", "堪萨斯城联储", "https://www.kansascityfed.org/"),
    ("minneapolis-fed", "明尼阿波利斯联储", "https://www.minneapolisfed.org/"),
    ("bank-of-england", "英国央行", "https://www.bankofengland.co.uk/"),
    ("ecb", "欧洲中央银行", "https://www.ecb.europa.eu/home/html/index.en.html"),
    ("bank-of-japan", "日本央行", "https://www.boj.or.jp/en/whatsnew/"),
    ("reserve-bank-australia", "澳洲联储", "https://www.rba.gov.au/"),
]

FEDERAL_RESERVE_BANK_IDS = frozenset({
    "boston-fed",
    "new-york-fed",
    "philadelphia-fed",
    "cleveland-fed",
    "richmond-fed",
    "atlanta-fed",
    "chicago-fed",
    "st-louis-fed",
    "minneapolis-fed",
    "kansas-city-fed",
    "dallas-fed",
    "san-francisco-fed",
})

FEDERAL_RESERVE_BOARD_FEEDS = (
    "https://www.federalreserve.gov/feeds/press_all.xml",
    "https://www.federalreserve.gov/feeds/speeches_and_testimony.xml",
    "https://www.federalreserve.gov/feeds/clp.xml",
    "https://www.federalreserve.gov/feeds/currentfaqs.xml",
    "https://www.federalreserve.gov/feeds/regreform.xml",
    "https://www.federalreserve.gov/feeds/boardmeetings.xml",
    "https://www.federalreserve.gov/feeds/reportforms-rss.xml",
    "https://www.federalreserve.gov/feeds/bankinginfo-rss.xml",
    "https://www.federalreserve.gov/feeds/covid-19.xml",
    "https://www.federalreserve.gov/feeds/working_papers.xml",
    "https://www.federalreserve.gov/feeds/datadownload.xml",
)

CHICAGO_FED_FEEDS = (
    "https://www.chicagofed.org/forms/rss/DataReleases",
    "https://www.chicagofed.org/forms/rss/NewsReleases",
    "https://www.chicagofed.org/forms/rss/Speeches",
    "https://www.chicagofed.org/forms/rss/cdps",
    "https://www.chicagofed.org/forms/rss/michiganeconomy",
    "https://www.chicagofed.org/forms/rss/midwesteconomy",
    "https://www.chicagofed.org/forms/rss/insights",
)

GLOBAL_CENTRAL_BANK_IDS = frozenset({
    "bank-of-england",
    "ecb",
    "bank-of-japan",
    "reserve-bank-australia",
})

BANK_OF_ENGLAND_FEEDS = (
    "https://www.bankofengland.co.uk/rss/bank-insights",
    "https://www.bankofengland.co.uk/rss/events",
    "https://www.bankofengland.co.uk/rss/knowledgebank",
    "https://www.bankofengland.co.uk/rss/news",
    "https://www.bankofengland.co.uk/rss/prudential-regulation-publications",
    "https://www.bankofengland.co.uk/rss/publications",
    "https://www.bankofengland.co.uk/rss/speeches",
    "https://www.bankofengland.co.uk/rss/statistics",
)

ECB_FEEDS = (
    "https://www.ecb.europa.eu/rss/press.html",
    "https://www.ecb.europa.eu/rss/blog.html",
    "https://www.ecb.europa.eu/rss/statpress.html",
    "https://www.ecb.europa.eu/rss/pub.html",
    "https://www.ecb.europa.eu/rss/wppub.html",
    "https://www.ecb.europa.eu/rss/operations.html",
    "https://www.ecb.europa.eu/rss/procurements.html",
    "https://www.ecb.europa.eu/rss/yc.html",
    "https://www.ecb.europa.eu/rss/rbu.html",
    "https://www.ecb.europa.eu/rss/tipsmeetdocs.rss",
)

BANK_OF_JAPAN_FEEDS = (
    "https://www.boj.or.jp/en/rss/whatsnew.xml",
    "https://www.boj.or.jp/en/rss/statistics.xml",
)

RESERVE_BANK_AUSTRALIA_FEEDS = (
    "https://www.rba.gov.au/rss/rss-cb-exchange-rates.xml",
    "https://www.rba.gov.au/rss/rss-cb-media-releases.xml",
    "https://www.rba.gov.au/rss/rss-cb-speeches.xml",
    "https://www.rba.gov.au/rss/rss-cb-speeches-webcast.xml",
    "https://www.rba.gov.au/rss/rss-cb-bulletin.xml",
    "https://www.rba.gov.au/rss/rss-cb-fsr.xml",
    "https://www.rba.gov.au/rss/rss-cb-smp.xml",
    "https://www.rba.gov.au/rss/rss-cb-rdp.xml",
    "https://www.rba.gov.au/rss/rss-cb-foi.xml",
    "https://www.rba.gov.au/rss/rss-cb-changes-to-tables.xml",
)

CATEGORIES = ("新闻", "智库", "央行")
SITES = NEWS_SITES + THINK_TANK_SITES + CENTRAL_BANK_SITES
SITE_CATEGORIES = {
    **{site_id: "新闻" for site_id, _, _ in NEWS_SITES},
    **{site_id: "智库" for site_id, _, _ in THINK_TANK_SITES},
    **{site_id: "央行" for site_id, _, _ in CENTRAL_BANK_SITES},
}

EXPLICIT_CHANNELS = [
    ("reuters", "sitemap", "https://www.reuters.com/arc/outboundfeeds/news-sitemap-index?outputType=xml"),
    ("investing", "feed", "https://www.investing.com/rss/news.rss"),
    ("politico", "feed", "https://rss.politico.com/politics-news.xml"),
    ("adbi", "feed", "https://www.adb.org/rss/adbi"),
    ("elcano", "feed", "https://www.realinstitutoelcano.org/en/feed/"),
    ("chatham-house", "feed", "https://www.chathamhouse.org/path/whatsnew.xml"),
    ("fabian-society", "feed", "https://fabians.org.uk/sitemap.rss"),
    ("federal-reserve", "homepage", "https://www.federalreserve.gov/newsevents.htm"),
    ("federal-reserve", "homepage", "https://www.federalreserve.gov/publications.htm"),
    *(("federal-reserve", "feed", url) for url in FEDERAL_RESERVE_BOARD_FEEDS),
    ("new-york-fed", "homepage", "https://www.newyorkfed.org/press"),
    ("chicago-fed", "homepage", "https://www.chicagofed.org/publications/publication-listing"),
    *(("chicago-fed", "feed", url) for url in CHICAGO_FED_FEEDS),
    ("atlanta-fed", "feed", "https://www.atlantafed.org/rss/listindex"),
    ("atlanta-fed", "feed", "https://www.atlantafed.org/rss/pressindex"),
    ("atlanta-fed", "feed", "https://www.atlantafed.org/rss/pubs"),
    ("atlanta-fed", "feed", "https://www.atlantafed.org/rss/speechindex"),
    ("dallas-fed", "feed", "https://www.dallasfed.org/rss/dallasfed.xml"),
    ("dallas-fed", "feed", "https://www.dallasfed.org/rss/releases.xml"),
    ("st-louis-fed", "feed", "https://www.stlouisfed.org/rss/page%20resources/publications/blog-entries"),
    ("st-louis-fed", "feed", "https://www.stlouisfed.org/rss/page%20resources/publications/open-vault-blog"),
    ("st-louis-fed", "feed", "https://www.stlouisfed.org/rss/page-resources/publications/page-one-economics"),
    ("st-louis-fed", "feed", "https://www.stlouisfed.org/rss/page-resources/publications/review"),
    ("st-louis-fed", "feed", "https://www.stlouisfed.org/rss/page%20resources/podcasts/timely-topics"),
    ("bank-of-england", "homepage", "https://www.bankofengland.co.uk/news"),
    ("bank-of-england", "homepage", "https://www.bankofengland.co.uk/publications"),
    *(("bank-of-england", "feed", url) for url in BANK_OF_ENGLAND_FEEDS),
    ("ecb", "homepage", "https://www.ecb.europa.eu/press/html/index.en.html"),
    ("ecb", "homepage", "https://www.ecb.europa.eu/pub/html/index.en.html"),
    *(("ecb", "feed", url) for url in ECB_FEEDS),
    ("bank-of-japan", "homepage", "https://www.boj.or.jp/en/about/calendar/"),
    *(("bank-of-japan", "feed", url) for url in BANK_OF_JAPAN_FEEDS),
    ("reserve-bank-australia", "homepage", "https://www.rba.gov.au/media-releases/"),
    ("reserve-bank-australia", "homepage", "https://www.rba.gov.au/publications/"),
    *(("reserve-bank-australia", "feed", url) for url in RESERVE_BANK_AUSTRALIA_FEEDS),
]

TARGETED_BACKFILLS = (
    {
        "run_id": "backfill-fomc-minutes-20260819",
        "site_id": "federal-reserve",
        "category": "央行",
        "url": "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260819a.htm",
        "title": "Minutes of the Federal Open Market Committee, July 28–29, 2026",
        "published_at": "2026-08-19T18:00:00+00:00",
    },
    {
        "run_id": "backfill-chicago-fed-letter-526",
        "site_id": "chicago-fed",
        "category": "央行",
        "url": "https://www.chicagofed.org/publications/chicago-fed-letter/2026/526",
        "title": "Central Clearing Mandates and Market Power: Lessons from Swaps for U.S. Treasury Securities",
        "published_at": "2026-08-20T00:00:00-05:00",
    },
)

OBSOLETE_CHANNELS = [
    ("reuters", "sitemap", "https://www.reuters.com/sitemap/2026-07/"),
    ("bank-of-england", "homepage", "https://www.bankofengland.co.uk/news/publications"),
    ("brookings", "feed", "https://www.brookings.edu/comments/feed"),
    ("brookings", "feed", "https://www.brookings.edu/feed"),
    ("fxstreet", "sitemap", "https://www.fxstreet.com/sitemap-all.xml"),
    ("foreignpolicy", "sitemap", "https://foreignpolicy.com/sitemap-100.xml"),
    ("foreignpolicy", "sitemap", "https://foreignpolicy.com/sitemap-101.xml"),
    ("foreignpolicy", "sitemap", "https://foreignpolicy.com/sitemap-102.xml"),
    ("cfr", "sitemap", "https://cfr.org/education/sitemap.xml"),
    ("atlantic-council", "sitemap", "https://n7initiative.org/sitemap_index.xml"),
    ("fabian-society", "sitemap", "https://fabians.org.uk/sitemap.rss"),
]

REMOVED_SITE_IDS = {
    "bloomberg-x", "truthsocial", "cbs",
    "ap", "businesstimes", "telegraph", "guardian-business",
    "scmp-business", "morningstar", "tradingeconomics", "straitstimes", "tradingview",
}

TRACKING_KEYS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "mod", "ref", "source", "output"}
SKIP_EXT = re.compile(r"\.(?:jpe?g|png|gif|webp|svg|ico|css|js|mjs|woff2?|ttf|eot|mp[34]|mov|avi|zip|gz|pdf)(?:$|\?)", re.I)
SKIP_PATH = re.compile(r"/(?:login|signin|signup|subscribe|account|privacy|terms|cookies?|contact|about|author|authors|tag|tags|topic|topics|category|search)(?:/|$)", re.I)
BASIC_SKIP_PATH = re.compile(r"/(?:login|signin|signup|subscribe|account|privacy|terms|cookies?|contact|about|author|authors|search)(?:/|$)", re.I)
FEED_TYPES = {"application/rss+xml", "application/atom+xml", "application/feed+json", "application/xml", "text/xml"}
XML_ENTITY_RE = re.compile(r"&([A-Za-z][A-Za-z0-9]+);")
XML_CORE_ENTITIES = {"amp", "lt", "gt", "quot", "apos"}


def utcnow() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def url_fingerprint(url: str) -> bytes:
    return hashlib.sha256(url.encode("utf-8")).digest()


def retention_cutoff() -> str:
    value = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=RETENTION_DAYS)
    return value.isoformat(timespec="seconds")


def current_news_dates(now: dt.datetime | None = None) -> set[dt.date]:
    now = now or dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    return {
        now.astimezone(ZoneInfo("Asia/Shanghai")).date(),
        now.astimezone(ZoneInfo("America/New_York")).date(),
    }


def parse_publication_time(value: str) -> dt.datetime | None:
    text = value.strip()
    if not text:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError):
        try:
            parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def publication_is_current(published: dt.datetime, now: dt.datetime | None = None) -> bool:
    now = now or dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    now = now.astimezone(dt.timezone.utc)
    starts = []
    for zone_name in ("Asia/Shanghai", "America/New_York"):
        zone = ZoneInfo(zone_name)
        local_date = now.astimezone(zone).date()
        starts.append(dt.datetime.combine(local_date, dt.time.min, tzinfo=zone).astimezone(dt.timezone.utc))
    return min(starts) <= published.astimezone(dt.timezone.utc) <= now + dt.timedelta(hours=6)


def url_has_non_current_date(url: str, today: dt.date | set[dt.date] | None = None) -> bool:
    """Hide dated URLs outside both the Beijing and US Eastern current dates."""
    allowed_dates = current_news_dates() if today is None else ({today} if isinstance(today, dt.date) else set(today))
    try:
        parsed = urllib.parse.urlsplit(urllib.parse.unquote(url))
        value = parsed.path + ("?" + parsed.query if parsed.query else "")
    except ValueError:
        return False
    full_patterns = (
        r"(?<!\d)(20\d{2})[-_/.](0?[1-9]|1[0-2])[-_/.](0?[1-9]|[12]\d|3[01])(?!\d)",
        r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)",
    )
    for pattern in full_patterns:
        for match in re.finditer(pattern, value):
            try:
                found = dt.date(*(int(part) for part in match.groups()))
            except ValueError:
                continue
            return found not in allowed_dates
    month_pattern = r"(?<!\d)(20\d{2})[-_/](0?[1-9]|1[0-2])(?![-_/]\d)(?!\d)"
    match = re.search(month_pattern, value)
    if match:
        allowed_months = {(value.year, value.month) for value in allowed_dates}
        return (int(match.group(1)), int(match.group(2))) not in allowed_months
    return False


def canonical_url(url: str, base: str | None = None) -> str | None:
    url = html.unescape(url.strip())
    if base:
        url = urllib.parse.urljoin(base, url)
    try:
        p = urllib.parse.urlsplit(url)
    except ValueError:
        return None
    if p.scheme not in {"http", "https"} or not p.netloc:
        return None
    host = (p.hostname or "").lower()
    port = f":{p.port}" if p.port and p.port not in {80, 443} else ""
    path = re.sub(r"/{2,}", "/", p.path or "/")
    if path != "/":
        path = path.rstrip("/")
    query = urllib.parse.parse_qsl(p.query, keep_blank_values=True)
    query = [(k, v) for k, v in query if k.lower() not in TRACKING_KEYS and not k.lower().startswith("utm_")]
    query.sort()
    return urllib.parse.urlunsplit((p.scheme.lower(), host + port, path, urllib.parse.urlencode(query), ""))


def same_site(candidate: str, home: str) -> bool:
    a = (urllib.parse.urlsplit(candidate).hostname or "").lower().removeprefix("www.")
    b = (urllib.parse.urlsplit(home).hostname or "").lower().removeprefix("www.")
    return a == b or a.endswith("." + b) or b.endswith("." + a)


def is_ignored_content_url(url: str) -> bool:
    try:
        parsed = urllib.parse.urlsplit(url)
    except ValueError:
        return True
    host = (parsed.hostname or "").lower().removeprefix("www.")
    path = urllib.parse.unquote(parsed.path).lower()
    quote_page_hosts = {"finance.yahoo.com", "finance.biggo.com"}
    if host in quote_page_hosts and (path == "/quote" or path.startswith("/quote/")):
        return True
    return False


def likely_page(url: str, home: str, category: str = "新闻") -> bool:
    p = urllib.parse.urlsplit(url)
    skip_path = SKIP_PATH if category == "新闻" else BASIC_SKIP_PATH
    return same_site(url, home) and not is_ignored_content_url(url) and not SKIP_EXT.search(url) and not skip_path.search(p.path) and url != canonical_url(home)


class PageParser(__import__("html.parser", fromlist=["HTMLParser"]).HTMLParser):
    def __init__(self, base: str, home: str, category: str = "新闻"):
        super().__init__(convert_charrefs=True)
        self.base, self.home, self.category = base, home, category
        self.links: dict[str, str] = {}
        self.feeds: set[str] = set()
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag, attrs):
        data = {k.lower(): (v or "") for k, v in attrs}
        if tag.lower() == "a" and data.get("href"):
            self._href = canonical_url(data["href"], self.base)
            self._text = []
        if tag.lower() == "link" and data.get("href"):
            rel = set(data.get("rel", "").lower().split())
            typ = data.get("type", "").lower().split(";", 1)[0]
            if "alternate" in rel and (typ in FEED_TYPES or "rss" in data["href"].lower() or "feed" in data["href"].lower()):
                value = canonical_url(data["href"], self.base)
                if value:
                    self.feeds.add(value)

    def handle_data(self, data):
        if self._href:
            self._text.append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self._href:
            if likely_page(self._href, self.home, self.category):
                title = re.sub(r"\s+", " ", " ".join(self._text)).strip()[:500]
                self.links.setdefault(self._href, title)
            self._href, self._text = None, []


class TitleParser(__import__("html.parser", fromlist=["HTMLParser"]).HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.meta_title = ""
        self.page_title: list[str] = []
        self.in_title = False

    def handle_starttag(self, tag, attrs):
        data = {k.lower(): (v or "") for k, v in attrs}
        if tag.lower() == "meta" and data.get("content"):
            key = (data.get("property") or data.get("name") or "").lower()
            if key in {"og:title", "twitter:title"} and not self.meta_title:
                self.meta_title = data["content"]
        elif tag.lower() == "title":
            self.in_title = True

    def handle_data(self, data):
        if self.in_title:
            self.page_title.append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "title":
            self.in_title = False

    def value(self) -> str:
        title = self.meta_title or " ".join(self.page_title)
        return re.sub(r"\s+", " ", html.unescape(title)).strip()[:500]


@dataclass
class FetchResult:
    ok: bool
    final_url: str
    content_type: str
    body: bytes = b""
    error: str = ""


def fetch(url: str, max_body: int = MAX_BODY) -> FetchResult:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/rss+xml,application/atom+xml,application/xml,text/xml,*/*;q=0.5", "Accept-Encoding": "gzip"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            body = resp.read(max_body + 1)
            if len(body) > max_body:
                body = body[:max_body]
            if resp.headers.get("Content-Encoding", "").lower() == "gzip":
                body = gzip.decompress(body)
            return FetchResult(True, resp.url, resp.headers.get_content_type(), body)
    except Exception as exc:
        return FetchResult(False, url, "", error=f"{type(exc).__name__}: {exc}"[:500])


def decode(body: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "gb18030", "latin-1"):
        try:
            return body.decode(enc)
        except UnicodeDecodeError:
            pass
    return body.decode("utf-8", "replace")


def fetch_page_title(url: str) -> tuple[str, str]:
    result = fetch(url, TITLE_FETCH_LIMIT)
    if not result.ok:
        return "", result.error
    try:
        parser = TitleParser()
        parser.feed(decode(result.body))
        title = parser.value()
        return (title, "") if title else ("", "页面没有可识别的标题")
    except Exception as exc:
        return "", f"标题解析失败: {type(exc).__name__}: {exc}"[:500]


def title_from_url(url: str) -> str:
    """Turn an article slug into a readable fallback title."""
    try:
        parts = [urllib.parse.unquote(part) for part in urllib.parse.urlsplit(url).path.split("/") if part]
    except ValueError:
        return ""
    ignored = {"article", "articles", "news", "story", "stories", "latest", "index"}
    candidates = [part for part in parts if part.lower() not in ignored]
    if not candidates:
        return ""
    slug = re.sub(r"\.(?:html?|shtml|php|aspx?)$", "", candidates[-1], flags=re.I)
    slug = re.sub(r"[-_+]+", " ", slug)
    slug = re.sub(r"\s+", " ", slug).strip(" .")
    if len(slug) < 4 or slug.isdigit() or re.fullmatch(r"[0-9a-f]{16,}", slug, re.I):
        return ""
    return (slug[:1].upper() + slug[1:])[:500]


def clean_title(title: str, url: str) -> str:
    value = re.sub(r"\s+", " ", html.unescape(title)).strip()
    value = re.sub(r"^\d{5,}\s+", "", value)
    value = re.sub(r"\s+\d{6,}$", "", value)
    value = re.sub(r"\s+[A-Za-z0-9_-]{12,}$", "", value)
    return value[:500] or title_from_url(url)


def likely_non_english_title(title: str) -> bool:
    if re.search(r"[\u0400-\u052f\u0590-\u08ff\u0900-\u0e7f\u3040-\u30ff\u3400-\u9fff\uac00-\ud7af]", title):
        return True
    words = set(re.findall(r"[a-zà-ÿ]+", title.lower()))
    language_markers = (
        {"el", "la", "los", "las", "del", "una", "para", "por", "con", "tras", "según", "campeonato", "policía", "aficionados", "refuerza", "fichar"},
        {"le", "les", "des", "une", "pour", "avec", "après", "selon", "dans", "aux", "sont"},
        {"der", "die", "das", "den", "dem", "eine", "mit", "für", "nach", "über", "sind"},
        {"uma", "para", "com", "após", "sobre", "segundo", "campeão", "polícia"},
        {"gli", "della", "delle", "una", "dopo", "sono", "sulla"},
    )
    return any(len(words & markers) >= 2 for markers in language_markers)


def translate_title_with_language(title: str) -> tuple[str, str, str]:
    if re.search(r"[\u3400-\u9fff]", title):
        return title, "", "zh"
    query = urllib.parse.urlencode({"client": "gtx", "sl": "auto", "tl": "zh-CN", "dt": "t", "q": title})
    endpoints = (
        "https://translate.googleapis.com/translate_a/single?",
        "https://translate.googleapis.com/translate_a/single?",
        "https://translate.google.com/translate_a/single?",
    )
    last_error = "翻译服务没有返回结果"
    for endpoint in endpoints:
        result = fetch(endpoint + query, 512 * 1024)
        if not result.ok:
            last_error = "翻译失败: " + result.error
            continue
        try:
            data = json.loads(decode(result.body))
            translated = "".join(part[0] for part in data[0] if part and part[0]).strip()
            language = data[2] if len(data) > 2 and isinstance(data[2], str) else ""
            if translated:
                return translated[:500], "", language.lower()
            last_error = "翻译服务没有返回结果"
        except Exception as exc:
            last_error = f"翻译解析失败: {type(exc).__name__}: {exc}"[:500]
    return "", last_error, ""


def translate_title(title: str) -> tuple[str, str]:
    translated, error, _ = translate_title_with_language(title)
    return translated, error


def localname(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def parse_public_xml(body: bytes) -> ET.Element:
    """Parse public XML while tolerating common publisher feed defects."""
    text = decode(body).lstrip("\ufeff \t\r\n")
    declaration = text.find("<?xml")
    if declaration > 0:
        text = text[declaration:]

    def replace_entity(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in XML_CORE_ENTITIES:
            return match.group(0)
        replacement = html.entities.html5.get(name + ";") or html.entities.html5.get(name)
        return replacement if replacement is not None else f"&amp;{name};"

    return ET.fromstring(XML_ENTITY_RE.sub(replace_entity, text))


def parse_feed_details(body: bytes, base: str, now: dt.datetime | None = None) -> tuple[dict[str, str], dict[str, str]]:
    root = parse_public_xml(body)
    items: dict[str, str] = {}
    published_dates: dict[str, str] = {}
    for node in root.iter():
        if localname(node.tag) not in {"item", "entry"}:
            continue
        title = ""
        link = None
        dates: dict[str, str] = {}
        for child in list(node):
            name = localname(child.tag)
            if name == "title" and child.text:
                title = re.sub(r"\s+", " ", child.text).strip()[:500]
            elif name == "link":
                href = child.attrib.get("href") or child.text
                rel = child.attrib.get("rel", "alternate")
                if href and rel in {"alternate", ""}:
                    link = canonical_url(href, base)
            elif name in {"guid", "id"} and not link and child.text and child.text.strip().startswith("http"):
                link = canonical_url(child.text, base)
            elif name in {"pubdate", "published", "issued", "date", "updated", "modified"} and child.text:
                dates.setdefault(name, child.text)
        date_text = next((dates[name] for name in ("pubdate", "published", "issued", "date", "updated", "modified") if name in dates), "")
        published = parse_publication_time(date_text) if date_text else None
        if published is not None and not publication_is_current(published, now):
            continue
        if link:
            items[link] = title
            if published is not None:
                published_dates[link] = published.isoformat(timespec="seconds")
    items = dict(list(items.items())[:MAX_ITEMS_PER_CHANNEL])
    return items, {url: published_dates[url] for url in items if url in published_dates}


def parse_feed(body: bytes, base: str, now: dt.datetime | None = None) -> dict[str, str]:
    return parse_feed_details(body, base, now)[0]


def candidate_is_current(url: str, published_at: str = "", now: dt.datetime | None = None) -> bool:
    if is_ignored_content_url(url):
        return False
    if published_at:
        published = parse_publication_time(published_at)
        if published is not None and publication_is_current(published, now):
            return True
    return not url_has_non_current_date(url, current_news_dates(now))


def parse_sitemap(body: bytes, base: str) -> tuple[dict[str, str], set[str]]:
    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    root = parse_public_xml(body)
    kind = localname(root.tag)
    pages: dict[str, str] = {}
    children: set[str] = set()
    if kind == "sitemapindex":
        for loc in root.iter():
            if localname(loc.tag) == "loc" and loc.text:
                value = canonical_url(loc.text, base)
                if value:
                    children.add(value)
    elif kind == "urlset":
        for url_node in list(root):
            loc = next((n.text for n in url_node.iter() if localname(n.tag) == "loc" and n.text), None)
            value = canonical_url(loc, base) if loc else None
            if value:
                pages[value] = ""
                if len(pages) >= MAX_ITEMS_PER_CHANNEL:
                    break
    else:
        raise ValueError("不是可识别的 sitemap XML")
    return pages, children


def parse_robots(body: bytes, base: str) -> set[str]:
    found = set()
    for line in decode(body).splitlines():
        if line.lower().startswith("sitemap:"):
            value = canonical_url(line.split(":", 1)[1].strip(), base)
            if value:
                found.add(value)
    return found


def sitemap_score(url: str) -> int:
    """Prefer news and recent sitemap shards over large historical archives."""
    value = url.lower()
    today = dt.datetime.now(dt.timezone.utc).date()
    score = 0
    if "news" in value:
        score += 60
    if "latest" in value or "recent" in value:
        score += 45
    if today.isoformat() in value:
        score += 120
    if today.strftime("%Y-%m") in value or today.strftime("%Y/%m") in value:
        score += 100
    if str(today.year) in value:
        score += 35
    if any(word in value for word in ("image", "video", "author", "tag", "category", "product")):
        score -= 80
    # Reuters paginates its live news sitemap from newest to oldest.  Without
    # this preference, the bounded channel set can retain arbitrary old pages.
    parts = urllib.parse.urlsplit(url)
    if parts.netloc.lower().endswith("reuters.com") and "/news-sitemap" in parts.path.lower():
        offset = urllib.parse.parse_qs(parts.query).get("from", ["0"])[0]
        try:
            score += max(0, 200 - int(offset) // 100)
        except ValueError:
            pass
    years = [int(x) for x in re.findall(r"(?:19|20)\d{2}", value)]
    if years and max(years) < today.year:
        score -= min(60, (today.year - max(years)) * 10)
    return score


def connect() -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db


def init_db() -> None:
    with connect() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS sites(id TEXT PRIMARY KEY, name TEXT NOT NULL, home_url TEXT NOT NULL, category TEXT NOT NULL DEFAULT '新闻');
        CREATE TABLE IF NOT EXISTS channels(
          id INTEGER PRIMARY KEY, site_id TEXT NOT NULL, kind TEXT NOT NULL, url TEXT NOT NULL,
          baseline_at TEXT, last_ok_at TEXT, last_error TEXT, last_count INTEGER NOT NULL DEFAULT 0,
          depth INTEGER NOT NULL DEFAULT 0, is_explicit INTEGER NOT NULL DEFAULT 0,
          UNIQUE(site_id, kind, url), FOREIGN KEY(site_id) REFERENCES sites(id));
        CREATE TABLE IF NOT EXISTS seen(
          channel_id INTEGER NOT NULL, url_hash BLOB NOT NULL, first_seen_at TEXT NOT NULL,
          PRIMARY KEY(channel_id, url_hash), FOREIGN KEY(channel_id) REFERENCES channels(id));
        CREATE TABLE IF NOT EXISTS runs(
          id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL,
          new_count INTEGER NOT NULL DEFAULT 0, ok_count INTEGER NOT NULL DEFAULT 0, error_count INTEGER NOT NULL DEFAULT 0,
          category TEXT NOT NULL DEFAULT '新闻');
        CREATE TABLE IF NOT EXISTS reports(
          id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, site_id TEXT NOT NULL, url TEXT NOT NULL,
          title TEXT NOT NULL DEFAULT '', title_zh TEXT NOT NULL DEFAULT '', enrich_error TEXT,
          enrich_attempts INTEGER NOT NULL DEFAULT 0, enriched_at TEXT, language TEXT NOT NULL DEFAULT '',
          published_at TEXT, channels TEXT NOT NULL, created_at TEXT NOT NULL,
          UNIQUE(url), FOREIGN KEY(run_id) REFERENCES runs(id));
        CREATE TABLE IF NOT EXISTS reported_fingerprints(
          url_hash BLOB PRIMARY KEY, first_reported_at TEXT NOT NULL);
        """)
        channel_columns = {row[1] for row in db.execute("PRAGMA table_info(channels)")}
        if "depth" not in channel_columns:
            db.execute("ALTER TABLE channels ADD COLUMN depth INTEGER NOT NULL DEFAULT 0")
        if "is_explicit" not in channel_columns:
            db.execute("ALTER TABLE channels ADD COLUMN is_explicit INTEGER NOT NULL DEFAULT 0")
        site_columns = {row[1] for row in db.execute("PRAGMA table_info(sites)")}
        if "category" not in site_columns:
            db.execute("ALTER TABLE sites ADD COLUMN category TEXT NOT NULL DEFAULT '新闻'")
        run_columns = {row[1] for row in db.execute("PRAGMA table_info(runs)")}
        if "category" not in run_columns:
            db.execute("ALTER TABLE runs ADD COLUMN category TEXT NOT NULL DEFAULT '新闻'")
        report_columns = {row[1] for row in db.execute("PRAGMA table_info(reports)")}
        if "title_zh" not in report_columns:
            db.execute("ALTER TABLE reports ADD COLUMN title_zh TEXT NOT NULL DEFAULT ''")
        if "enrich_error" not in report_columns:
            db.execute("ALTER TABLE reports ADD COLUMN enrich_error TEXT")
        if "enrich_attempts" not in report_columns:
            db.execute("ALTER TABLE reports ADD COLUMN enrich_attempts INTEGER NOT NULL DEFAULT 0")
        if "enriched_at" not in report_columns:
            db.execute("ALTER TABLE reports ADD COLUMN enriched_at TEXT")
        if "language" not in report_columns:
            db.execute("ALTER TABLE reports ADD COLUMN language TEXT NOT NULL DEFAULT ''")
        if "published_at" not in report_columns:
            db.execute("ALTER TABLE reports ADD COLUMN published_at TEXT")
        migrate_seen_to_fingerprints(db)
        db.executemany(
            "INSERT OR IGNORE INTO reported_fingerprints(url_hash,first_reported_at) VALUES(?,?)",
            [(url_fingerprint(row["url"]), row["created_at"]) for row in db.execute("SELECT url,created_at FROM reports")],
        )
        site_rows = [(site_id, name, url, SITE_CATEGORIES[site_id]) for site_id, name, url in SITES]
        db.executemany("INSERT INTO sites(id,name,home_url,category) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,home_url=excluded.home_url,category=excluded.category", site_rows)
        db.executemany("INSERT OR IGNORE INTO channels(site_id,kind,url,is_explicit) VALUES(?, 'homepage', ?, 1)", [(x[0], canonical_url(x[2])) for x in SITES])
        db.execute("UPDATE channels SET is_explicit=1 WHERE kind='homepage'")
        db.executemany("INSERT OR IGNORE INTO channels(site_id,kind,url,is_explicit) VALUES(?,?,?,1)", [(s, k, canonical_url(u)) for s, k, u in EXPLICIT_CHANNELS])
        db.executemany("UPDATE channels SET is_explicit=1 WHERE site_id=? AND kind=? AND url=?", [(s, k, canonical_url(u)) for s, k, u in EXPLICIT_CHANNELS])
        apply_targeted_backfills(db)
        for site_id, kind, url in OBSOLETE_CHANNELS:
            obsolete_url = canonical_url(url)
            db.execute("DELETE FROM seen WHERE channel_id IN (SELECT id FROM channels WHERE site_id=? AND kind=? AND url=?)", (site_id, kind, obsolete_url))
            db.execute("DELETE FROM channels WHERE site_id=? AND kind=? AND url=?", (site_id, kind, obsolete_url))
        remove_configured_sites(db)
        db.execute("UPDATE runs SET status='interrupted',finished_at=? WHERE status='running'", (utcnow(),))
        prune_sitemaps(db)
        cleanup_history(db)
        remove_historical_dated_reports(db)
        remove_ignored_reports(db)


def migrate_seen_to_fingerprints(db: sqlite3.Connection) -> None:
    columns = {row[1] for row in db.execute("PRAGMA table_info(seen)")}
    if "url" not in columns:
        return
    db.execute("""
      CREATE TABLE IF NOT EXISTS seen_compact(
        channel_id INTEGER NOT NULL, url_hash BLOB NOT NULL, first_seen_at TEXT NOT NULL,
        PRIMARY KEY(channel_id,url_hash))
    """)
    cursor = db.execute("SELECT channel_id,url,first_seen_at FROM seen")
    while True:
        rows = cursor.fetchmany(10_000)
        if not rows:
            break
        db.executemany(
            "INSERT OR IGNORE INTO seen_compact(channel_id,url_hash,first_seen_at) VALUES(?,?,?)",
            [(row["channel_id"], url_fingerprint(row["url"]), row["first_seen_at"]) for row in rows],
        )
    db.execute("DROP TABLE seen")
    db.execute("ALTER TABLE seen_compact RENAME TO seen")


def cleanup_history(db: sqlite3.Connection) -> tuple[int, int]:
    cutoff = retention_cutoff()
    old_reports = db.execute("SELECT url,created_at FROM reports WHERE created_at<?", (cutoff,)).fetchall()
    db.executemany(
        "INSERT OR IGNORE INTO reported_fingerprints(url_hash,first_reported_at) VALUES(?,?)",
        [(url_fingerprint(row["url"]), row["created_at"]) for row in old_reports],
    )
    report_count = db.execute("DELETE FROM reports WHERE created_at<?", (cutoff,)).rowcount
    run_count = db.execute("DELETE FROM runs WHERE COALESCE(finished_at,started_at)<?", (cutoff,)).rowcount
    return report_count, run_count


def remove_configured_sites(db: sqlite3.Connection) -> int:
    removed = 0
    for site_id in REMOVED_SITE_IDS:
        reports = db.execute("SELECT url,created_at FROM reports WHERE site_id=?", (site_id,)).fetchall()
        db.executemany(
            "INSERT OR IGNORE INTO reported_fingerprints(url_hash,first_reported_at) VALUES(?,?)",
            [(url_fingerprint(row["url"]), row["created_at"]) for row in reports],
        )
        db.execute("DELETE FROM reports WHERE site_id=?", (site_id,))
        db.execute("DELETE FROM seen WHERE channel_id IN (SELECT id FROM channels WHERE site_id=?)", (site_id,))
        db.execute("DELETE FROM channels WHERE site_id=?", (site_id,))
        removed += db.execute("DELETE FROM sites WHERE id=?", (site_id,)).rowcount
    return removed


def apply_targeted_backfills(db: sqlite3.Connection) -> int:
    inserted = 0
    for item in TARGETED_BACKFILLS:
        fingerprint = url_fingerprint(item["url"])
        if db.execute("SELECT 1 FROM reported_fingerprints WHERE url_hash=?", (fingerprint,)).fetchone():
            continue
        now = utcnow()
        db.execute(
            "INSERT OR IGNORE INTO runs(id,started_at,finished_at,status,new_count,ok_count,error_count,category) VALUES(?,?,?,'done',0,1,0,?)",
            (item["run_id"], now, now, item["category"]),
        )
        marker = db.execute(
            "INSERT OR IGNORE INTO reported_fingerprints(url_hash,first_reported_at) VALUES(?,?)",
            (fingerprint, now),
        )
        if not marker.rowcount:
            continue
        report = db.execute(
            "INSERT OR IGNORE INTO reports(run_id,site_id,url,title,published_at,channels,created_at) VALUES(?,?,?,?,?,?,?)",
            (item["run_id"], item["site_id"], item["url"], item["title"], item["published_at"], '["feed"]', now),
        )
        inserted += report.rowcount
        db.execute("UPDATE runs SET new_count=? WHERE id=?", (report.rowcount, item["run_id"]))
    return inserted


def remove_historical_dated_reports(db: sqlite3.Connection) -> int:
    recent_publication_cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=RETENTION_DAYS)
    rows = []
    for row in db.execute("SELECT id,url,created_at,published_at FROM reports"):
        if not url_has_non_current_date(row["url"]):
            continue
        published = parse_publication_time(row["published_at"] or "")
        if published is not None and published >= recent_publication_cutoff:
            continue
        rows.append(row)
    db.executemany(
        "INSERT OR IGNORE INTO reported_fingerprints(url_hash,first_reported_at) VALUES(?,?)",
        [(url_fingerprint(row["url"]), row["created_at"]) for row in rows],
    )
    db.executemany("DELETE FROM reports WHERE id=?", [(row["id"],) for row in rows])
    return len(rows)


def remove_ignored_reports(db: sqlite3.Connection) -> int:
    rows = [row for row in db.execute("SELECT id,url,created_at FROM reports") if is_ignored_content_url(row["url"])]
    db.executemany(
        "INSERT OR IGNORE INTO reported_fingerprints(url_hash,first_reported_at) VALUES(?,?)",
        [(url_fingerprint(row["url"]), row["created_at"]) for row in rows],
    )
    db.executemany("DELETE FROM reports WHERE id=?", [(row["id"],) for row in rows])
    return len(rows)


def cleanup_local_files() -> None:
    cutoff_timestamp = time.time() - RETENTION_DAYS * 86400
    for path in ROOT.glob("news_monitor.pre_*.sqlite3"):
        try:
            if path.stat().st_mtime < cutoff_timestamp:
                path.unlink()
        except OSError:
            pass
    log_path = ROOT / "news_monitor.log"
    try:
        if log_path.exists() and log_path.stat().st_mtime < cutoff_timestamp:
            log_path.write_text("", encoding="utf-8")
    except OSError:
        pass


def maintenance_loop() -> None:
    while True:
        threading.Event().wait(3600)
        cleanup_local_files()
        try:
            with connect() as db:
                cleanup_history(db)
        except sqlite3.Error:
            pass


def prune_sitemaps(db: sqlite3.Connection) -> int:
    """Keep explicit channels plus the best bounded set of discovered sitemaps."""
    removed = 0
    site_ids = [row[0] for row in db.execute("SELECT id FROM sites")]
    for site_id in site_ids:
        rows = db.execute("SELECT id,url,is_explicit FROM channels WHERE site_id=? AND kind='sitemap'", (site_id,)).fetchall()
        if len(rows) <= MAX_SITEMAPS_PER_SITE:
            continue
        explicit = [row for row in rows if row["is_explicit"]]
        discovered = sorted((row for row in rows if not row["is_explicit"]), key=lambda row: (-sitemap_score(row["url"]), row["id"]))
        keep = explicit + discovered[:max(0, MAX_SITEMAPS_PER_SITE - len(explicit))]
        keep_ids = [row["id"] for row in keep]
        placeholders = ",".join("?" for _ in keep_ids)
        params = [site_id, *keep_ids]
        doomed = f"SELECT id FROM channels WHERE site_id=? AND kind='sitemap' AND id NOT IN ({placeholders})"
        removed += db.execute(f"SELECT COUNT(*) FROM ({doomed})", params).fetchone()[0]
        db.execute(f"DELETE FROM seen WHERE channel_id IN ({doomed})", params)
        db.execute(f"DELETE FROM channels WHERE site_id=? AND kind='sitemap' AND id NOT IN ({placeholders})", params)
    return removed


def add_channel(db: sqlite3.Connection, site_id: str, kind: str, url: str, depth: int = 0, explicit: bool = False) -> bool:
    value = canonical_url(url)
    if not value:
        return False
    if any(
        site_id == old_site and kind == old_kind and value == canonical_url(old_url)
        for old_site, old_kind, old_url in OBSOLETE_CHANNELS
    ):
        return False
    if kind == "sitemap" and not explicit:
        if depth > MAX_SITEMAP_DEPTH:
            return False
    cur = db.execute("INSERT OR IGNORE INTO channels(site_id,kind,url,depth,is_explicit) VALUES(?,?,?,?,?)", (site_id, kind, value, depth, int(explicit)))
    return cur.rowcount == 1


def discover_for_site(site: sqlite3.Row) -> None:
    """Discover public feed and sitemap URLs; failures do not affect channel baselines."""
    home = site["home_url"]
    base = f"{urllib.parse.urlsplit(home).scheme}://{urllib.parse.urlsplit(home).netloc}/"
    home_result, robots_result = fetch(home), fetch(urllib.parse.urljoin(base, "robots.txt"))
    feeds: set[str] = set()
    maps: set[str] = set()
    if home_result.ok:
        try:
            parser = PageParser(home_result.final_url, home, site["category"])
            parser.feed(decode(home_result.body))
            feeds |= parser.feeds
        except Exception:
            pass
    if robots_result.ok:
        maps |= parse_robots(robots_result.body, base)
    with connect() as db:
        for url in feeds:
            add_channel(db, site["id"], "feed", url)
        for url in maps:
            if same_site(url, home):
                add_channel(db, site["id"], "sitemap", url)


def collect_channel(row: sqlite3.Row, home_url: str) -> tuple[int, bool, dict[str, str], set[str], dict[str, str], str]:
    result = fetch(row["url"])
    if not result.ok:
        return row["id"], False, {}, set(), {}, result.error
    try:
        if row["kind"] == "homepage":
            parser = PageParser(result.final_url, home_url, row["category"])
            parser.feed(decode(result.body))
            return row["id"], True, parser.links, set(), {}, ""
        if row["kind"] == "feed":
            items, published_dates = parse_feed_details(result.body, result.final_url)
            return row["id"], True, items, set(), published_dates, ""
        pages, children = parse_sitemap(result.body, result.final_url)
        return row["id"], True, pages, children, {}, ""
    except Exception as exc:
        return row["id"], False, {}, set(), {}, f"解析失败: {type(exc).__name__}: {exc}"[:500]


refresh_lock = threading.Lock()
enrichment_lock = threading.Lock()
refresh_state = {
    "running": False, "run_id": None, "message": "", "started_at": None,
    "phase": "", "completed": 0, "total": 0, "percent": 0, "category": "",
}


def enrich_one(row: sqlite3.Row) -> tuple[int, str, str, str, str]:
    title = clean_title(row["title"], row["url"]) if row["title"] else ""
    errors: list[str] = []
    if not title:
        title = title_from_url(row["url"])
        error = ""
        if not title:
            title, error = fetch_page_title(row["url"])
        if error and not title:
            errors.append(error)
    title_zh = ""
    language = ""
    if title:
        if likely_non_english_title(title):
            language = "non-en"
        else:
            title_zh, error, language = translate_title_with_language(title)
            if error:
                errors.append(error)
    if not title_zh:
        title_zh = row["title_zh"]
    return row["id"], title, title_zh, "；".join(errors)[:500], language


def enrich_reports(limit: int = MAX_ENRICH_PER_PASS, site_id: str | None = None) -> int:
    if not enrichment_lock.acquire(blocking=False):
        return 0
    try:
        with connect() as db:
            site_clause = " AND site_id=?" if site_id else ""
            params = (site_id, limit) if site_id else (limit,)
            rows = db.execute(f"""
              SELECT id,url,title,title_zh FROM reports
              WHERE (title_zh='' OR language='') AND enrich_attempts<3{site_clause}
              ORDER BY created_at DESC,id DESC LIMIT ?
            """, params).fetchall()
        if not rows:
            return 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            results = list(pool.map(enrich_one, rows))
        with connect() as db:
            now = utcnow()
            for report_id, title, title_zh, error, language in results:
                if language and language.split("-", 1)[0] != "en":
                    db.execute("DELETE FROM reports WHERE id=?", (report_id,))
                    continue
                db.execute("""
                  UPDATE reports SET title=?,title_zh=?,enrich_error=?,language=?,
                    enrich_attempts=enrich_attempts+1,enriched_at=? WHERE id=?
                """, (title, title_zh, error or None, language, now, report_id))
        return sum(bool(row[2]) and (not row[4] or row[4].split("-", 1)[0] == "en") for row in results)
    finally:
        enrichment_lock.release()


def pending_enrichment_count(site_id: str | None = None) -> int:
    with connect() as db:
        site_clause = " AND site_id=?" if site_id else ""
        params = (site_id,) if site_id else ()
        return db.execute(
            f"SELECT COUNT(*) FROM reports WHERE (title_zh='' OR language='') AND enrich_attempts<3{site_clause}",
            params,
        ).fetchone()[0]


def drain_enrichment(site_id: str | None = None, progress_callback=None) -> int:
    initial = pending_enrichment_count(site_id)
    if not initial:
        return 0
    enriched = 0
    max_rounds = (initial // 100 + 2) * 3
    for _ in range(max_rounds):
        before = pending_enrichment_count(site_id)
        if not before:
            break
        enriched += enrich_reports(limit=100, site_id=site_id)
        after = pending_enrichment_count(site_id)
        if progress_callback:
            progress_callback(initial - after, initial)
    return enriched


def run_refresh(run_id: str, category: str) -> None:
    started = utcnow()
    try:
        with connect() as db:
            db.execute("INSERT INTO runs(id,started_at,status,category) VALUES(?,?,'running',?)", (run_id, started, category))
            sites = db.execute("SELECT * FROM sites WHERE category=? ORDER BY name", (category,)).fetchall()
        refresh_state.update(message=f"正在刷新【{category}】：发现 RSS/Atom 与 sitemap…", phase="发现采集通道", completed=0, total=len(sites), percent=1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [pool.submit(discover_for_site, site) for site in sites]
            for completed, future in enumerate(concurrent.futures.as_completed(futures), 1):
                try:
                    future.result()
                except Exception:
                    pass
                refresh_state.update(completed=completed, total=len(sites), percent=max(1, round(completed / len(sites) * 15)))

        pending: list[sqlite3.Row]
        with connect() as db:
            prune_sitemaps(db)
            pending = db.execute("SELECT c.*,s.home_url,s.category FROM channels c JOIN sites s ON s.id=c.site_id WHERE s.category=? ORDER BY c.id", (category,)).fetchall()
        refresh_state.update(message=f"正在检查 {len(pending)} 个采集通道…", phase="检查采集通道", completed=0, total=len(pending), percent=15)
        processed: set[int] = set()
        candidates: dict[str, dict] = {}
        ok_count = error_count = 0

        while pending:
            batch = [r for r in pending if r["id"] not in processed]
            pending = []
            if not batch:
                break
            refresh_state["message"] = f"正在检查采集通道…"
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = [pool.submit(collect_channel, row, row["home_url"]) for row in batch]
                results = []
                for batch_done, future in enumerate(concurrent.futures.as_completed(futures), 1):
                    results.append(future.result())
                    done = len(processed) + batch_done
                    total = max(refresh_state["total"], len(processed) + len(batch))
                    percent = min(90, 15 + round(75 * done / max(1, total)))
                    refresh_state.update(completed=done, total=total, percent=max(refresh_state["percent"], percent))
            by_id = {r["id"]: r for r in batch}
            with connect() as db:
                for channel_id, ok, items, children, published_dates, error in results:
                    row = by_id[channel_id]
                    processed.add(channel_id)
                    if not ok:
                        error_count += 1
                        db.execute("UPDATE channels SET last_error=?,last_count=0 WHERE id=?", (error, channel_id))
                        continue
                    ok_count += 1
                    now = utcnow()
                    is_baseline = row["baseline_at"] is None
                    old = {bytes(x[0]) for x in db.execute("SELECT url_hash FROM seen WHERE channel_id=?", (channel_id,))}
                    new_items = [(url, title) for url, title in items.items() if url_fingerprint(url) not in old]
                    db.executemany("INSERT OR IGNORE INTO seen(channel_id,url_hash,first_seen_at) VALUES(?,?,?)", [(channel_id, url_fingerprint(u), now) for u, _ in new_items])
                    db.execute("UPDATE channels SET baseline_at=COALESCE(baseline_at,?),last_ok_at=?,last_error=NULL,last_count=? WHERE id=?", (now, now, len(items), channel_id))
                    if not is_baseline:
                        for url, title in new_items:
                            entry = candidates.setdefault(url, {"site_id": row["site_id"], "title": title, "channels": set(), "published_at": ""})
                            if title and not entry["title"]:
                                entry["title"] = title
                            if published_dates.get(url):
                                entry["published_at"] = published_dates[url]
                            entry["channels"].add(row["kind"])
                    ranked_children = sorted(children, key=lambda url: (-sitemap_score(url), url))
                    for child in ranked_children[:MAX_SITEMAP_CHILDREN]:
                        add_channel(db, row["site_id"], "sitemap", child, depth=row["depth"] + 1)
                prune_sitemaps(db)
                if any(children for _, _, _, children, _, _ in results):
                    pending = db.execute("SELECT c.*,s.home_url,s.category FROM channels c JOIN sites s ON s.id=c.site_id WHERE c.kind='sitemap' AND s.category=?", (category,)).fetchall()

        inserted = 0
        refresh_state.update(message="正在整理新增链接…", phase="生成报告", completed=0, total=len(candidates), percent=92)
        with connect() as db:
            now = utcnow()
            for url, data in candidates.items():
                if not candidate_is_current(url, data["published_at"]):
                    continue
                marker = db.execute("INSERT OR IGNORE INTO reported_fingerprints(url_hash,first_reported_at) VALUES(?,?)", (url_fingerprint(url), now))
                if marker.rowcount:
                    cur = db.execute("INSERT OR IGNORE INTO reports(run_id,site_id,url,title,published_at,channels,created_at) VALUES(?,?,?,?,?,?,?)", (run_id, data["site_id"], url, data["title"], data["published_at"] or None, json.dumps(sorted(data["channels"]), ensure_ascii=False), now))
                    inserted += cur.rowcount
            db.execute("UPDATE runs SET finished_at=?,status='done',new_count=?,ok_count=?,error_count=? WHERE id=?", (utcnow(), inserted, ok_count, error_count, run_id))
            cleanup_history(db)
        refresh_state.update(message="正在补充网页标题和中文翻译…", phase="补充标题与翻译", completed=0, total=min(MAX_ENRICH_PER_PASS, inserted), percent=95)
        def enrichment_progress(done, total):
            percent = min(99, 95 + round(4 * done / max(1, total)))
            refresh_state.update(completed=done, total=total, percent=percent)

        enriched = drain_enrichment(progress_callback=enrichment_progress)
        with connect() as db:
            kept = db.execute("SELECT COUNT(*) FROM reports WHERE run_id=?", (run_id,)).fetchone()[0]
            db.execute("UPDATE runs SET new_count=? WHERE id=?", (kept, run_id))
        refresh_state.update(message=f"【{category}】完成：发现 {kept} 条英文新增链接，补充 {enriched} 条中文标题", phase="完成", completed=kept, total=kept, percent=100)
    except Exception as exc:
        with connect() as db:
            db.execute("UPDATE runs SET finished_at=?,status='failed' WHERE id=?", (utcnow(), run_id))
        refresh_state["message"] = f"刷新失败：{type(exc).__name__}: {exc}"
    finally:
        refresh_state["running"] = False
        refresh_lock.release()


def start_refresh(category: str = "新闻") -> tuple[bool, str]:
    if category not in CATEGORIES:
        raise ValueError("未知分类")
    if not refresh_lock.acquire(blocking=False):
        return False, str(refresh_state.get("run_id") or "")
    run_id = uuid.uuid4().hex
    refresh_state.update(running=True, run_id=run_id, message=f"准备刷新【{category}】…", started_at=utcnow(), phase="准备", completed=0, total=0, percent=0, category=category)
    threading.Thread(target=run_refresh, args=(run_id, category), daemon=True).start()
    return True, run_id


def state_payload() -> dict:
    with connect() as db:
        sites = [dict(r) for r in db.execute("""
          SELECT s.id,s.name,s.home_url,s.category,
            COUNT(c.id) channel_count,
            SUM(CASE WHEN c.baseline_at IS NOT NULL THEN 1 ELSE 0 END) baseline_count,
            SUM(CASE WHEN c.last_error IS NOT NULL THEN 1 ELSE 0 END) error_count,
            MAX(c.last_ok_at) last_ok_at
          FROM sites s LEFT JOIN channels c ON c.site_id=s.id GROUP BY s.id ORDER BY s.name
        """)]
        channels = [dict(r) for r in db.execute("SELECT c.*,s.name site_name,s.category FROM channels c JOIN sites s ON s.id=c.site_id ORDER BY s.category,s.name,c.kind,c.url")]
        reports = [dict(r) for r in db.execute("SELECT r.*,s.name site_name,s.category FROM reports r JOIN sites s ON s.id=r.site_id ORDER BY r.created_at DESC,r.id DESC LIMIT 2000")]
        runs = [dict(r) for r in db.execute("SELECT * FROM runs ORDER BY started_at DESC LIMIT 30")]
    for r in reports:
        r["channels"] = json.loads(r["channels"])
    return {"refresh": dict(refresh_state), "sites": sites, "channels": channels, "reports": reports, "runs": runs}


class Handler(BaseHTTPRequestHandler):
    def send_json(self, value, status=200):
        body = json.dumps(value, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/api/state":
            return self.send_json(state_payload())
        if self.path in {"/", "/index.html"}:
            body = (ROOT / "index.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return self.wfile.write(body)
        self.send_error(404)

    def do_POST(self):
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/api/refresh":
            category = urllib.parse.parse_qs(parsed.query).get("category", ["新闻"])[0]
            if category not in CATEGORIES:
                return self.send_json({"error": "未知分类"}, 400)
            started, run_id = start_refresh(category)
            return self.send_json({"started": started, "run_id": run_id, "category": category}, 202 if started else 409)
        self.send_error(404)

    def log_message(self, fmt, *args):
        if args and str(args[0]).startswith("GET /api/state"):
            return
        super().log_message(fmt, *args)


def main() -> None:
    if not (ROOT / "index.html").is_file():
        raise SystemExit(f"缺少网页界面文件：{ROOT / 'index.html'}")
    cleanup_local_files()
    init_db()
    threading.Thread(target=drain_enrichment, daemon=True).start()
    threading.Thread(target=maintenance_loop, daemon=True).start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"新闻链接监控已启动：http://{HOST}:{PORT}")
    print("按 Ctrl+C 停止。数据库保存在 news_monitor.sqlite3。")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止。")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
