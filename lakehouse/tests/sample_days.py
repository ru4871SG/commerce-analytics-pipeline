"""Three days of storefront exports with every case the two engines must treat alike.

New York changes to summer time on 8 March 2026 at 02:00, so the days have different UTC spans:
    7 March  05:00 UTC .. 8 March 05:00 UTC
    8 March  05:00 UTC .. 9 March 04:00 UTC   (23 hours)
    9 March  04:00 UTC .. 10 March 04:00 UTC

    s1  broomx, facebook, mobile: a whole funnel to a purchase, engaged by 12 s of engagement;
        its first page view is sent twice in the same file
    s2  broomx, no utm (direct), desktop: starts at 23:50 on 7 March and goes on after midnight.
        Its second page view arrives in 8 March's file with a re-send of its first, so 7 March is
        rebuilt by the second batch, and the session becomes engaged (two page views) only then
    s3  wecare, " Google " (spaces, capitals), tablet: across the summer-time change; a variant id
        sent as a string
    s4  wecare, newsletter, no device: a bounce, not engaged; customer_id 1e3 is not an integer
    s5  greenz, instagram, mobile: a 7 March session delivered late, in 8 March's file
    s6  greenz, direct, " Mobile ": 9 March, with two events on one line (a JSON array), one
        followed by text after the object, params sent as a JSON string, and odd customer ids;
        two of its events are sent again in a second file of the day
    and in 9 March's file, lines every rule drops.
"""
from __future__ import annotations

import gzip
import json
import os

DAYS = ("2026-03-07", "2026-03-08", "2026-03-09")


def event(event_id, session_id, brand, name, time, **fields) -> dict:
    return {"event_id": event_id, "event_time": time, "event_name": name, "brand": brand,
            "session_id": session_id, "client_id": f"c-{session_id}", **fields}


def line(value) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


S1 = dict(brand="broomx", utm_source="facebook", utm_medium="cpc", utm_campaign="prospecting", device="mobile",
          page_path="/products/peak-whey", user_agent="Mozilla/5.0 (iPhone)")
S2 = dict(brand="broomx", device="desktop", page_path="/")
S3 = dict(brand="wecare", utm_source=" Google ", utm_medium="cpc", device="tablet")
S4 = dict(brand="wecare", utm_source="newsletter", utm_medium="email", customer_id="1e3")
S5 = dict(brand="greenz", utm_source="instagram", device="mobile")
S6 = dict(brand="greenz", device=" Mobile ", customer_id="+5")


def _e(event_id, session, fields, name, time, **extra):
    values = {**fields, **extra}
    brand = values.pop("brand")
    return event(event_id, session, brand, name, time, **values)


def day_files() -> dict[str, list[str]]:
    """The files of each day: {relative path: lines}."""
    s1 = [
        _e("e01", "s1", S1, "page_view", "2026-03-07T15:00:00.000Z"),
        _e("e02", "s1", S1, "view_item", "2026-03-07T15:01:00.000Z", params={"variant_id": 3}),
        _e("e03", "s1", S1, "user_engagement", "2026-03-07T15:01:30.000Z", params={"engagement_time_msec": 12000}),
        _e("e04", "s1", S1, "add_to_cart", "2026-03-07T15:02:00.000Z", params={"variant_id": 3, "quantity": 1}),
        _e("e05", "s1", S1, "begin_checkout", "2026-03-07T15:03:00.000Z", params={"items": [3]}),
        _e("e06", "s1", S1, "purchase", "2026-03-07T15:05:00.000Z", customer_id=42,
           params={"order_number": "SM-1", "items": [3], "value_cents": 1850}),
    ]
    s2_first = _e("e07", "s2", S2, "page_view", "2026-03-08T04:50:00.000Z")
    day1 = [line(s1[0]), *map(line, s1), line(s2_first)]

    day2 = [
        line(s2_first),                                              # re-sent
        line(_e("e08", "s2", S2, "page_view", "2026-03-08T05:05:00.000Z")),
        line(_e("e09", "s3", S3, "page_view", "2026-03-08T06:30:00.000Z")),
        line(_e("e10", "s3", S3, "view_item", "2026-03-08T06:31:00.000Z", params={"variant_id": "17"})),
        line(_e("e11", "s3", S3, "page_view", "2026-03-08T07:10:00.000Z")),
        line(_e("e12", "s4", S4, "page_view", "2026-03-08T15:00:00.000Z")),
        line(_e("e13", "s4", S4, "user_engagement", "2026-03-08T15:00:05.000Z", params={"engagement_time_msec": 3000})),
        # s5 is a 7 March session, delivered a day late
        line(_e("e14", "s5", S5, "page_view", "2026-03-07T20:00:00.000Z")),
        line(_e("e15", "s5", S5, "scroll", "2026-03-07T20:00:30.000Z", params={"percent_scrolled": 90})),
        line(_e("e16", "s5", S5, "user_engagement", "2026-03-07T20:01:00.000Z", params={"engagement_time_msec": 15000})),
    ]

    e17 = _e("e17", "s6", S6, "page_view", "2026-03-09T12:00:00.000Z")
    e18 = _e("e18", "s6", S6, "add_to_cart", "2026-03-09T12:01:00.000Z", params={"variant_id": 5, "quantity": 1})
    day3 = [
        line(e17),
        line(e18),
        line([_e("e19", "s6", S6, "page_view", "2026-03-09T12:02:00.000Z"),
              _e("e20", "s6", S6, "scroll", "2026-03-09T12:02:10.000Z", params={"percent_scrolled": 50})]),
        line(_e("e21", "s6", S6, "user_engagement", "2026-03-09T12:03:00.000Z",
                params={"engagement_time_msec": 4000, "ratio": 1.50})) + " trailing text",
        line(_e("e22", "s6", S6, "view_item", "2026-03-09T12:04:00.000Z", page_path="/productos/café",
                customer_id="٤٢", params=json.dumps({"variant_id": 9}))),
        line(_e("e23", "s6", S6, "page_view", "2026-03-09T12:05:00.000Z", customer_id=7, params=5)),
        # Lines every rule drops, in RULES order
        '{"event_id":"x01","event_time":"2026-03-09T',                                       # parsable
        "{}",                                                                                # has_ids
        line(event("x02", "", "greenz", "page_view", "2026-03-09T12:00:00.000Z")),            # has_ids
        line(event("x03", "s6", "  ", "page_view", "2026-03-09T12:00:00.000Z")),             # has_ids
        line(event("x04", "s6", "greenz", "debug_ping", "2026-03-09T12:00:00.000Z")),         # known_event_name
        line(event("x05", "s6", "greenz", "page_view", "2026-03-09 12:00:00.000Z")),          # valid_event_time
        line(event("x06", "s6", "greenz", "page_view", "2026-02-30T12:00:00.000Z")),          # valid_event_time
        line(event("x07", "s6", "greenz", "page_view", "1999-12-31T23:59:59.999Z")),          # valid_event_time
        "",
    ]
    retry = [line(e17), line(e18)]                                   # the collector sent these twice
    return {
        "2026-03-07/broomx-2026-03-07.json.gz": day1,
        "2026-03-08/mixed-2026-03-08.json.gz": day2,
        "2026-03-09/greenz-2026-03-09.json.gz": day3,
        "2026-03-09/greenz-2026-03-09-retry.json.gz": retry,
    }


def land(landing: str, day: str) -> list[str]:
    """Write the day's files into the landing folder (gzip, a temporary name renamed into place,
    as the collector does). Returns their paths."""
    written = []
    for relative, lines in day_files().items():
        if not relative.startswith(day):
            continue
        path = os.path.join(landing, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        temporary = os.path.join(os.path.dirname(path), "." + os.path.basename(path))
        with gzip.open(temporary, "wt", encoding="utf-8") as out:
            out.write("\n".join(lines) + "\n")
        os.replace(temporary, path)
        written.append(path)
    return written
