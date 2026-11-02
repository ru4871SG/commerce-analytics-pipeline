"""Write the storefronts' clickstream export into the landing volume. Demo only.
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import multiprocessing
import os
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from crm_pipeline import source_postgres
from crm_pipeline.config import LAKEHOUSE_CATALOG, LAKEHOUSE_ROOT, TIMEZONE, Brand, brands_to_extract

log = logging.getLogger(__name__)

NEW_YORK = ZoneInfo(TIMEZONE)
FIRST_DAY = date(2025, 1, 1)                # the clickstream export starts with the shops
SESSIONS_PER_WEB_ORDER = 200                # ~0.5% of sessions convert
TRAILING_DAYS = 28                          # traffic follows the average order rate of the four weeks before
ORGANIC_MIX = (("direct", None, 0.55), ("instagram", "social", 0.30), ("newsletter", "email", 0.15))
LATE_SESSION_SHARE = 0.01                   # app sessions delivered with the next day's file
DUPLICATE_SHARE = 0.004                     # events the collector sends twice
RETRY_FILE_SHARE = 0.05                     # days with a second file re-sending part of the day
RETRY_EVENT_SHARE = 0.02
JUNK_PER_EVENTS = 5_000                     # one line the rules drop per this many events
DEVICES = (("mobile", 0.62), ("desktop", 0.30), ("tablet", 0.08))
USER_AGENTS = {
    "mobile": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Mobile/15E148 Safari/604.1",
    "desktop": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "tablet": "Mozilla/5.0 (iPad; CPU OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Mobile/15E148 Safari/604.1",
    "app": "GreenzShopApp/4.12 (Android 16; Pixel 9)",
}


@dataclass(frozen=True)
class Order:
    number: str
    customer_id: int | None
    created_at: datetime                    # UTC, aware
    utm_source: str | None
    utm_medium: str | None
    utm_campaign: str | None
    variants: tuple[int, ...]


@dataclass
class DayInput:
    orders: list[Order] = field(default_factory=list)
    carts: list[Order] = field(default_factory=list)
    clicks: list[tuple[str, str, str, int]] = field(default_factory=list)   # source, medium, campaign, clicks
    trailing_web_orders: float = 0.0       # storefront orders a day, over the TRAILING_DAYS before


@dataclass
class BrandInputs:
    brand: str
    shop_domain: str
    products: dict[int, str]               # variant id -> product page path
    days: dict[date, DayInput]


def landing_dir(catalog: str = LAKEHOUSE_CATALOG, root: str = LAKEHOUSE_ROOT) -> str:
    return os.path.join(root, "Volumes", catalog, "bronze", "landing", "web_events")


def day_path(landing: str, brand: str, day: date, suffix: str = "") -> str:
    return os.path.join(landing, day.isoformat(), f"{brand}-{day.isoformat()}{suffix}.json.gz")


def _utc_start(day: date) -> datetime:
    """Midnight of a New York day, in UTC."""
    return datetime.combine(day, time(), NEW_YORK).astimezone(timezone.utc)


# --- what the CRM holds --------------------------------------------------------------------

def load_inputs(brand: Brand, first: date, last: date) -> BrandInputs:
    """The brand's storefront orders, carts and ad clicks from first to last, from its CRM, with
    the four weeks before for the trailing order rate."""
    since, until = _utc_start(first - timedelta(days=TRAILING_DAYS)), _utc_start(last + timedelta(days=1))
    with source_postgres.connect(brand.source_database) as conn, conn.cursor() as cursor:
        cursor.execute(
            "select v.id, p.code from product_variants v join products p on p.id = v.product_id order by v.id")
        products = {variant: f"/products/{code}" for variant, code in cursor.fetchall()}
        cursor.execute(
            "select acquisition_domain from orders where acquisition_domain like 'shop.%%' "
            "group by 1 order by count(*) desc limit 1")
        domain = (cursor.fetchone() or (f"shop.{brand.key}.example",))[0]
        cursor.execute(
            """
            select o.number, o.customer_id, o.state, o.created_at, o.utm_source, o.utm_medium, o.utm_campaign,
                   coalesce(array_agg(i.variant_id order by i.id) filter (where i.variant_id is not null), '{}')
            from orders o left join order_items i on i.order_id = o.id
            where o.created_from_order_id is null and not o.is_created_by_admin
              and o.created_at >= %s and o.created_at < %s
            group by o.id order by o.created_at, o.id
            """,
            (since.replace(tzinfo=None), until.replace(tzinfo=None)),
        )
        orders = cursor.fetchall()
        cursor.execute(
            "select spend_date, utm_source, utm_medium, utm_campaign, sum(clicks) from marketing_spend "
            "where spend_date between %s and %s group by 1, 2, 3, 4 order by 1, 2, 3, 4",
            (first, last),
        )
        clicks = cursor.fetchall()

    days = {first + timedelta(days=n): DayInput() for n in range((last - first).days + 1)}
    web_orders_per_day: dict[date, int] = defaultdict(int)
    for number, customer_id, state, created_at, source, medium, campaign, variants in orders:
        moment = created_at.replace(tzinfo=timezone.utc)
        day = moment.astimezone(NEW_YORK).date()
        order = Order(number, customer_id, moment, source, medium, campaign, tuple(variants))
        if state != "cart":
            web_orders_per_day[day] += 1
        if day in days:
            (days[day].carts if state == "cart" else days[day].orders).append(order)
    for day, source, medium, campaign, count in clicks:
        if day in days and count:
            days[day].clicks.append((source, medium, campaign, int(count)))
    for day, inputs in days.items():
        inputs.trailing_web_orders = sum(
            web_orders_per_day.get(day - timedelta(days=n), 0) for n in range(1, TRAILING_DAYS + 1)) / TRAILING_DAYS
    return BrandInputs(brand.key, domain, products, days)


# --- one day's events ----------------------------------------------------------------------

def _pick(rng: random.Random, weighted) -> str:
    roll, total = rng.random(), 0.0
    for value, weight in weighted:
        total += weight
        if roll < total:
            return value
    return weighted[-1][0]


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


class _Session:
    """Events of one session, in the order they happen. Times are filled in at the end."""

    def __init__(self, rng, inputs: BrandInputs, utm, device, client_id, customer_id=None):
        self.rng, self.inputs = rng, inputs
        self.session_id = f"{rng.getrandbits(128):032x}"
        self.utm, self.device, self.client_id, self.customer_id = utm, device, client_id, customer_id
        self.steps: list[tuple[str, str, dict]] = []

    def add(self, name: str, path: str, params: dict | None = None):
        self.steps.append((name, path, params or {}))

    def page(self, path: str, engaged_ms: tuple[int, int]):
        domain = self.inputs.shop_domain
        self.add("page_view", path, {"page_location": f"https://{domain}{path}"})
        if self.rng.random() < 0.6:
            self.add("scroll", path, {"percent_scrolled": 90})
        self.add("user_engagement", path, {"engagement_time_msec": self.rng.randint(*engaged_ms)})

    def browse(self, pages: int):
        variants = list(self.inputs.products)
        for _ in range(pages):
            if self.rng.random() < 0.45:
                shown = self.rng.sample(variants, k=min(len(variants), self.rng.randint(4, 12)))
                self.page("/collections/all", (2_000, 25_000))
                self.add("view_item_list", "/collections/all", {"item_list_id": "all", "items": shown})
            else:
                variant = self.rng.choice(variants)
                self.page(self.inputs.products[variant], (3_000, 40_000))
                self.add("view_item", self.inputs.products[variant], {"variant_id": variant})

    def events(self, brand: str, start: datetime, end: datetime | None = None) -> list[dict]:
        """The events, from start, a few seconds to a minute apart; with end, spread to finish on
        it exactly (a purchase happens when the order was created)."""
        gaps = [self.rng.uniform(4, 60) for _ in self.steps[1:]]
        if end is not None and gaps:
            scale = max(0.0, (end - start).total_seconds()) / sum(gaps)
            gaps = [gap * scale for gap in gaps]
        moment, events = start, []
        for index, (name, path, params) in enumerate(self.steps):
            if index:
                moment = moment + timedelta(seconds=gaps[index - 1])
            if end is not None and index == len(self.steps) - 1:
                moment = end
            event = {
                "event_id": f"{self.rng.getrandbits(128):032x}", "event_time": _iso(moment), "event_name": name,
                "brand": brand, "session_id": self.session_id, "client_id": self.client_id,
                "page_path": path, "device": self.device, "user_agent": USER_AGENTS[self.device],
            }
            if self.customer_id is not None:
                event["customer_id"] = self.customer_id
            source, medium, campaign = self.utm
            if source:
                event.update(utm_source=source, utm_medium=medium, utm_campaign=campaign)
            if params:
                event["params"] = params
            events.append(event)
        return events


def _client(rng: random.Random, brand: str) -> str:
    # three visits in ten come from a returning browser
    if rng.random() < 0.3:
        return f"{brand}-{rng.randrange(20_000):05d}"
    return f"{rng.getrandbits(64):016x}"


def _start_in_day(rng: random.Random, day: date) -> datetime:
    """A moment of the New York day, weighted towards the afternoon and evening."""
    hour = min(23, max(0, int(rng.gauss(15, 4.5))))
    local = datetime.combine(day, time(hour, rng.randrange(60), rng.randrange(60), rng.randrange(1000) * 1000), NEW_YORK)
    return local.astimezone(timezone.utc)


def sessions_for_day(inputs: BrandInputs, day: date, scale: float = 1.0) -> int:
    """How many sessions the brand's storefront has on the day (orders and carts included)."""
    day_input = inputs.days[day]
    return max(round(day_input.trailing_web_orders * SESSIONS_PER_WEB_ORDER * scale),
               len(day_input.orders) + len(day_input.carts))


def day_events(inputs: BrandInputs, day: date, scale: float = 1.0) -> list[dict]:
    """The day's events, in time order."""
    rng = random.Random(f"{inputs.brand}|{day.isoformat()}|day")
    day_input = inputs.days[day]
    brand, events = inputs.brand, []
    day_start, day_end = _utc_start(day), _utc_start(day + timedelta(days=1))

    for order in day_input.orders:
        session = _Session(rng, inputs, (order.utm_source, order.utm_medium, order.utm_campaign),
                           _pick(rng, DEVICES), f"{brand}-c{order.customer_id}", order.customer_id)
        session.page(inputs.products.get(order.variants[0], "/") if order.variants else "/", (5_000, 30_000))
        session.browse(rng.randint(0, 3))
        for variant in order.variants:
            session.add("view_item", inputs.products.get(variant, "/"), {"variant_id": variant})
            session.add("add_to_cart", inputs.products.get(variant, "/"), {"variant_id": variant, "quantity": 1})
        session.add("begin_checkout", "/checkout", {"items": list(order.variants)})
        session.add("purchase", "/checkout/thank-you", {"order_number": order.number, "items": list(order.variants)})
        # The session starts the same New York day as the order: dbt reconciles them by day
        start = max(order.created_at - timedelta(minutes=rng.uniform(4, 30)), day_start + timedelta(seconds=1))
        events += session.events(brand, start, order.created_at)

    for cart in day_input.carts:
        session = _Session(rng, inputs, (cart.utm_source, cart.utm_medium, cart.utm_campaign),
                           _pick(rng, DEVICES), f"{brand}-c{cart.customer_id}", cart.customer_id)
        session.browse(rng.randint(1, 3))
        for variant in cart.variants:
            session.add("view_item", inputs.products.get(variant, "/"), {"variant_id": variant})
            session.add("add_to_cart", inputs.products.get(variant, "/"), {"variant_id": variant, "quantity": 1})
        if rng.random() < 0.5:
            session.add("begin_checkout", "/checkout", {"items": list(cart.variants)})
        events += session.events(brand, max(cart.created_at - timedelta(minutes=10), day_start + timedelta(seconds=1)))

    # The ad clicks that did not convert, then direct, social and newsletter visits up to the day's traffic
    converted = defaultdict(int)
    for order in day_input.orders:
        converted[order.utm_source] += 1
    visits = []
    for source, medium, campaign, clicks in day_input.clicks:
        visits += [(source, medium, campaign)] * max(0, round(clicks * scale) - converted[source])
        converted[source] = 0
    organic = max(0, sessions_for_day(inputs, day, scale) - len(day_input.orders) - len(day_input.carts) - len(visits))
    for _ in range(organic):
        source, medium, _weight = rng.choices(ORGANIC_MIX, weights=[w for *_, w in ORGANIC_MIX])[0]
        visits.append((None, None, None) if source == "direct" else (source, medium, f"{source}_{day:%Y_%m}"))
    for utm in visits:
        session = _Session(rng, inputs, utm, _pick(rng, DEVICES), _client(rng, brand))
        landing = rng.choice(list(inputs.products.values())) if utm[0] in ("facebook", "google", "tiktok") else "/"
        if rng.random() < 0.45:                               # a bounce: one page, a few seconds
            session.add("page_view", landing, {"page_location": f"https://{inputs.shop_domain}{landing}"})
            session.add("user_engagement", landing, {"engagement_time_msec": rng.randint(500, 9_000)})
        else:
            session.page(landing, (3_000, 30_000))
            session.browse(min(12, 1 + int(rng.expovariate(1 / 2.5))))
            if rng.random() < 0.08:
                variant = rng.choice(list(inputs.products))
                session.add("add_to_cart", inputs.products[variant], {"variant_id": variant, "quantity": 1})
                if rng.random() < 0.35:
                    session.add("begin_checkout", "/checkout", {"items": [variant]})
        start = _start_in_day(rng, day)
        events += [event for event in session.events(brand, start) if event["event_time"] < _iso(day_end)]
    return sorted(events, key=lambda event: event["event_time"])


def late_sessions(inputs: BrandInputs, day: date, scale: float = 1.0) -> list[dict]:
    """App sessions of the day that the phone delivered the next day, with that day's file."""
    rng = random.Random(f"{inputs.brand}|{day.isoformat()}|late")
    events = []
    for _ in range(max(1, round(sessions_for_day(inputs, day, scale) * LATE_SESSION_SHARE))):
        session = _Session(rng, inputs, (None, None, None), "app", _client(rng, inputs.brand))
        session.page("/", (2_000, 20_000))
        session.browse(rng.randint(1, 4))
        events += session.events(inputs.brand, _start_in_day(rng, day))
    return events


def _junk(rng: random.Random, event: dict) -> str:
    """A line the lakehouse's rules drop, made from a real event."""
    bad = dict(event, event_id=f"{rng.getrandbits(128):032x}")
    kind = rng.randrange(4)
    if kind == 0:
        text = json.dumps(bad, separators=(",", ":"))
        return text[: len(text) // 2]                         # truncated: not JSON
    if kind == 1:
        bad["event_name"] = "debug_ping"
    elif kind == 2:
        bad.pop("session_id")
    else:
        bad["event_time"] = bad["event_time"].replace("T", " ")
    return json.dumps(bad, separators=(",", ":"), ensure_ascii=False)


def file_lines(inputs: BrandInputs, day: date, scale: float = 1.0) -> tuple[list[str], list[str]]:
    """The lines of the day's file, and of its retry file (empty on most days)."""
    rng = random.Random(f"{inputs.brand}|{day.isoformat()}|collector")
    events = day_events(inputs, day, scale)
    previous = day - timedelta(days=1)
    if previous in inputs.days and previous >= FIRST_DAY:
        events += late_sessions(inputs, previous, scale)
    lines = [json.dumps(event, separators=(",", ":"), ensure_ascii=False) for event in events]
    browsing = [index for index, event in enumerate(events) if event["event_name"] != "purchase"]
    extra = [lines[index] for index in browsing if rng.random() < DUPLICATE_SHARE]
    # One junk line per JUNK_PER_EVENTS events on average: a file of a few thousand gets one now and then
    junk = min(len(browsing), int(len(events) / JUNK_PER_EVENTS + rng.random()))
    extra += [_junk(rng, events[index]) for index in rng.sample(browsing, k=junk)]
    for line in extra:
        lines.insert(rng.randrange(len(lines) + 1), line)
    retry = []
    if rng.random() < RETRY_FILE_SHARE:
        retry = [json.dumps(events[index], separators=(",", ":"), ensure_ascii=False)
                 for index in browsing if rng.random() < RETRY_EVENT_SHARE]
    return lines, retry


def _write(path: str, lines: list[str]) -> int:
    """Write the lines gzipped under a temporary name, then rename: the planner never sees a
    file half-written. Returns the compressed size."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temporary = os.path.join(os.path.dirname(path), "." + os.path.basename(path))
    with open(temporary, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, compresslevel=6) as out:
        out.write(("\n".join(lines) + "\n").encode())
    os.replace(temporary, path)
    return os.path.getsize(path)


def write_day(inputs: BrandInputs, day: date, landing: str, scale: float = 1.0) -> dict:
    path = day_path(landing, inputs.brand, day)
    if os.path.exists(path):
        return {"brand": inputs.brand, "day": day.isoformat(), "written": False}
    lines, retry = file_lines(inputs, day, scale)
    size = _write(path, lines)
    if retry:
        size += _write(day_path(landing, inputs.brand, day, "-retry"), retry)
    return {"brand": inputs.brand, "day": day.isoformat(), "written": True, "files": 2 if retry else 1,
            "lines": len(lines) + len(retry), "bytes": size}


def export_day(brand: Brand, day: date, landing: str | None = None, scale: float = 1.0) -> dict:
    """The brand's file for one day: the daily activity DAG and asset call this after the
    simulator has written the day into the CRM."""
    # The day before too: its late app sessions go into this day's file
    result = write_day(load_inputs(brand, day - timedelta(days=1), day), day, landing or landing_dir(), scale)
    log.info("%s %s clickstream: %s", brand.key, day, result)
    return result


_INPUTS: dict[str, BrandInputs] = {}


def _write_task(task: tuple[str, date, str, float]) -> dict:
    brand, day, landing, scale = task
    return write_day(_INPUTS[brand], day, landing, scale)


def export_range(brands: list[Brand], first: date, last: date, landing: str | None = None, scale: float = 1.0,
                 workers: int | None = None) -> list[dict]:
    """Every brand's files from first to last, in parallel: the history the lakehouse backfills."""
    landing = landing or landing_dir()
    first = max(first, FIRST_DAY)
    for brand in brands:
        _INPUTS[brand.key] = load_inputs(brand, first, last)
    tasks = [(brand.key, first + timedelta(days=n), landing, scale)
             for brand in brands for n in range((last - first).days + 1)]
    workers = workers or min(8, os.cpu_count() or 1)
    if workers == 1:
        return [_write_task(task) for task in tasks]
    # Forked workers share the inputs read above instead of receiving a copy each
    with multiprocessing.get_context("fork").Pool(workers) as pool:
        return pool.map(_write_task, tasks, chunksize=8)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write the storefronts' clickstream export (demo only).")
    parser.add_argument("--from", dest="first", type=date.fromisoformat, default=FIRST_DAY)
    parser.add_argument("--to", dest="last", type=date.fromisoformat,
                        default=datetime.now(NEW_YORK).date() - timedelta(days=1))
    parser.add_argument("--scale", type=float, default=float(os.environ.get("CRM_CLICKSTREAM_SCALE", "1")))
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--brand", action="append", help="only these brands (default: every extracted brand)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    brands = [brand for brand in brands_to_extract() if not args.brand or brand.key in args.brand]
    results = export_range(brands, args.first, args.last, scale=args.scale, workers=args.workers)
    written = [result for result in results if result["written"]]
    # A brand-day can be two files: the day's, and a -retry file re-sending part of it
    print(f"Clickstream {args.first}..{args.last}: {sum(r['files'] for r in written)} files written for "
          f"{len(written)} brand-days ({sum(r['lines'] for r in written):,} lines, "
          f"{sum(r['bytes'] for r in written) / 2**20:,.1f} MiB), {len(results) - len(written)} brand-days "
          f"already there, in {landing_dir()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
