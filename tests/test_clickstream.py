"""The demo's clickstream collector: one story with the CRM (a purchase per storefront order, on
its New York day), the same file every time, and the collector's imperfections in the amounts
the lakehouse's rules expect. Built from inputs in memory: no database."""
from __future__ import annotations

import gzip
import json
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from crm_pipeline import clickstream
from crm_pipeline.clickstream import BrandInputs, DayInput, Order

DAY = date(2026, 3, 8)                  # New York changes to summer time that night
NEW_YORK = ZoneInfo("America/New_York")


def inputs() -> BrandInputs:
    orders = [
        # 00:03 New York, so the session cannot start the day before
        Order("SM-1", 11, datetime(2026, 3, 8, 5, 3, tzinfo=timezone.utc), "facebook", "cpc", "prospecting", (1, 2)),
        Order("SM-2", 12, datetime(2026, 3, 8, 20, 30, tzinfo=timezone.utc), None, None, None, (3,)),
    ]
    carts = [Order("SM-3", 13, datetime(2026, 3, 8, 16, 0, tzinfo=timezone.utc), "google", "cpc", "brand", (2,))]
    days = {d: DayInput(trailing_web_orders=2.0) for d in (date(2026, 3, 7), DAY)}
    days[DAY] = DayInput(orders=orders, carts=carts, clicks=[("tiktok", "cpc", "prospecting", 20)], trailing_web_orders=2.0)
    return BrandInputs("broomx", "shop.broomx.example", {1: "/products/a", 2: "/products/b", 3: "/products/c"}, days)


def test_every_storefront_order_is_one_purchase_on_its_new_york_day():
    events = clickstream.day_events(inputs(), DAY)
    purchases = [e for e in events if e["event_name"] == "purchase"]
    assert sorted(e["params"]["order_number"] for e in purchases) == ["SM-1", "SM-2"]
    for purchase in purchases:
        session = [e for e in events if e["session_id"] == purchase["session_id"]]
        first = min(e["event_time"] for e in session)
        start = datetime.fromisoformat(first.replace("Z", "+00:00")).astimezone(NEW_YORK)
        assert start.date() == DAY                                  # dbt reconciles them by day
        assert max(e["event_time"] for e in session) == purchase["event_time"]
    sm1 = next(p for p in purchases if p["params"]["order_number"] == "SM-1")
    assert sm1["event_time"] == "2026-03-08T05:03:00.000Z" and sm1["utm_source"] == "facebook"
    assert "utm_source" not in next(p for p in purchases if p["params"]["order_number"] == "SM-2")


def test_traffic_follows_the_orders_and_the_ad_clicks():
    events = clickstream.day_events(inputs(), DAY)
    sessions = {e["session_id"]: e for e in events}
    assert len(sessions) == clickstream.sessions_for_day(inputs(), DAY) == 2 * clickstream.SESSIONS_PER_WEB_ORDER
    tiktok = {s for s, e in sessions.items() if e.get("utm_source") == "tiktok"}
    assert len(tiktok) == 20                                        # one session per ad click
    assert {e["event_name"] for e in events} <= {"page_view", "view_item_list", "view_item", "add_to_cart",
                                                 "begin_checkout", "purchase", "scroll", "user_engagement"}


def test_a_day_comes_out_the_same_every_time():
    first, _ = clickstream.file_lines(inputs(), DAY)
    again, _ = clickstream.file_lines(inputs(), DAY)
    assert first == again
    assert first != clickstream.file_lines(inputs(), date(2026, 3, 7))[0]


def test_the_file_carries_the_day_before_s_late_app_sessions_and_a_little_junk():
    lines, _ = clickstream.file_lines(inputs(), DAY)
    events = clickstream.day_events(inputs(), DAY) + clickstream.late_sessions(inputs(), date(2026, 3, 7))
    clean = {json.dumps(e, separators=(",", ":"), ensure_ascii=False) for e in events}
    late = [e for e in events if e["device"] == "app"]
    assert late and all(datetime.fromisoformat(e["event_time"].replace("Z", "+00:00")).astimezone(NEW_YORK).date()
                        == date(2026, 3, 7) for e in late)
    # Besides the events: re-sent copies of some, and one line the rules drop per JUNK_PER_EVENTS
    junk = [line for line in lines if line not in clean]
    assert len(events) // clickstream.JUNK_PER_EVENTS <= len(junk) <= len(events) // clickstream.JUNK_PER_EVENTS + 1
    assert len(lines) - len(junk) - len(events) == sum(1 for line in lines if line in clean) - len(clean)
    assert sum(1 for line in lines if '"event_name":"purchase"' in line) == 2       # never re-sent nor junk


def test_a_written_day_is_never_rewritten(tmp_path):
    landing = str(tmp_path)
    first = clickstream.write_day(inputs(), DAY, landing)
    path = clickstream.day_path(landing, "broomx", DAY)
    stamp = open(path, "rb").read()
    assert first["written"] and not clickstream.write_day(inputs(), DAY, landing)["written"]
    # The day's file, and its -retry file when part of the day is re-sent
    assert first["files"] == 1 + bool(clickstream.file_lines(inputs(), DAY)[1])
    assert open(path, "rb").read() == stamp
    with gzip.open(path, "rt") as lines:
        assert sum(1 for _ in lines) == first["lines"] - (len(clickstream.file_lines(inputs(), DAY)[1]))
