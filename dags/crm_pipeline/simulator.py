"""Simulate one day of shop activity in a brand's CRM database. Demo only.
"""
from __future__ import annotations

import logging
import math
import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from crm_pipeline import source_postgres
from crm_pipeline.config import SOURCE_WRITER_CONN_ID, TIMEZONE
from crm_pipeline.errors import PermanentError

log = logging.getLogger(__name__)

FIRST_DAY = date(2026, 10, 1)       # the seed data in fake_crm.sql ends on 30 September 2026
ID_START, ID_END = 20_000, 70_000   # simulated rows use ids 20000-69999 in every table
IDS_PER_DAY = 50                    # per table and day: 1,000 days fit in the range
SCRIPTED_CUSTOMERS = 8              # customers 1-8 are the seed's edge cases

# Share of due subscriptions cancelled instead of renewed, as in generate_fake_crm.py: the first
# renewals lose the most subscribers, so the days after the seed do not look like better retention
FIRST_RENEWALS = 2
FIRST_RENEWALS_CHURN = 0.20
LATER_RENEWALS_CHURN = 0.08
# The days of sign-ups a day's rate averages. Over a month, a few slow days lower the days after
# them, so a brand can sink: every day is seeded, so it would then sink on every install.
SIGN_UP_RATE_DAYS = 90
REBUY_PER_DAY = 0.006               # daily chance a recent one-time customer orders again (~0.7/day, as in September)
REFUND_RATE = 0.05                  # share of a week-old order day that is refunded
CART_PER_DAY = 0.30
TABLES = ("customers", "orders", "order_items", "refund_payments", "marketing_spend")


def business_day(logical_date) -> date:
    """The 07:00 run on day D records the shop activity of D-1, New York time."""
    moment = logical_date
    if not isinstance(moment, datetime):
        moment = datetime.combine(moment, time())
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(ZoneInfo(TIMEZONE)).date() - timedelta(days=1)


def id_block(day: date) -> range:
    """The ids the day owns in every table."""
    if day < FIRST_DAY:
        raise PermanentError(f"{day} is before {FIRST_DAY}: the seed data already covers it.")
    start = ID_START + (day - FIRST_DAY).days * IDS_PER_DAY
    if start + IDS_PER_DAY > ID_END:
        raise PermanentError(f"The simulated id range {ID_START}-{ID_END - 1} is used up on {day}.")
    return range(start, start + IDS_PER_DAY)


@dataclass(frozen=True)
class Variant:
    id: int
    product_name: str
    code: str
    price: int

    @property
    def purchase_type(self) -> str:      # as stg_crm__product_variants decodes it
        if "vip" in self.code:
            return "Subscribe"
        return "Upsell" if "-u" in self.code else "One-Time Purchase"


@dataclass(frozen=True)
class Subscription:
    order_id: int
    customer_id: int
    variant_id: int
    last_order_at: datetime
    renewals: int               # how many times it has renewed so far


@dataclass(frozen=True)
class Budget:
    """A campaign's average day of ad spend over the last four weeks."""

    utm_source: str
    utm_medium: str
    utm_campaign: str
    amount: float               # cents
    impressions: float
    clicks: float


@dataclass
class DayState:
    """What the day starts from, read from the CRM."""

    prefix: str                 # order number prefix, e.g. SM
    currency: str
    country: str
    shop_domain: str
    shipping_cents: int
    variants: list[Variant]
    new_customers_per_day: float
    subscriptions: list[Subscription]
    rebuy_customers: list[int]
    refund_candidates: list[tuple[int, str, int]]       # (order id, number, total)
    budgets: list[Budget] = field(default_factory=list)


@dataclass
class DayPlan:
    customers: list[dict] = field(default_factory=list)
    orders: list[dict] = field(default_factory=list)
    order_items: list[dict] = field(default_factory=list)
    refund_payments: list[dict] = field(default_factory=list)
    marketing_spend: list[dict] = field(default_factory=list)
    cancellations: list[tuple[int, datetime]] = field(default_factory=list)   # (order id, cancelled at)
    refunded_orders: list[tuple[int, str]] = field(default_factory=list)      # (order id, payment state)
    counts: dict[str, int] = field(default_factory=dict)


def _poisson(rng: random.Random, mean: float) -> int:
    limit, k, p = math.exp(-mean), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def _moment(rng: random.Random, day: date) -> datetime:
    return datetime.combine(day, time(rng.randint(8, 22), rng.randint(0, 59), rng.randint(0, 59)))


def _states(rng: random.Random) -> tuple[str, str, str]:
    roll = rng.random()
    if roll < 0.03:
        return "cancelled", "cancelled", "cancelled"
    if roll < 0.04:
        return "new", "awaiting_payment", "ready"
    return "fulfilled", "paid", "shipped"


def plan_day(state: DayState, day: date, rng: random.Random, now: datetime) -> DayPlan:
    """Decide the day's activity. Pure: no database access, so it can be tested on its own."""
    block = id_block(day)
    plan = DayPlan()
    variants = {v.id: v for v in state.variants}
    by_type = {t: [v for v in state.variants if v.purchase_type == t]
               for t in ("Subscribe", "Upsell", "One-Time Purchase")}
    pending = []    # orders before ids are assigned: (created_at, order fields, [(variant, quantity)])

    def add_order(created_at, customer_id, lines, **fields):
        pending.append((created_at, {"customer_id": customer_id, **fields}, lines))

    for sub in state.subscriptions:
        interval = 28 + sub.order_id % 5
        if sub.last_order_at.date() + timedelta(days=interval) > day or sub.variant_id not in variants:
            continue
        churn = FIRST_RENEWALS_CHURN if sub.renewals < FIRST_RENEWALS else LATER_RENEWALS_CHURN
        if rng.random() < churn:
            plan.cancellations.append((sub.order_id, _moment(rng, day)))
        else:
            add_order(_moment(rng, day), sub.customer_id, [(variants[sub.variant_id], 1)],
                      created_from_order_id=sub.order_id)

    for _ in range(_poisson(rng, state.new_customers_per_day)):
        if len(plan.customers) == len(block):
            break
        customer_id = block[len(plan.customers)]
        signed_up = _moment(rng, day)
        plan.customers.append({
            "id": customer_id, "email": f"customer{customer_id}@example.com", "country_code": state.country,
            "subscribed_to_newsletter": rng.random() < 0.55, "created_at": signed_up, "updated_at": now,
        })
        subscription = rng.random() < 0.5 and bool(by_type["Subscribe"])
        lines = [(rng.choice(by_type["Subscribe" if subscription else "One-Time Purchase"]), 1 if rng.random() < 0.8 else 2)]
        if by_type["Upsell"] and rng.random() < 0.15:
            lines.append((rng.choice(by_type["Upsell"]), 1))
        roll = rng.random()
        acquisition = (
            {"is_sales_funnel": True, "acquisition_domain": state.shop_domain, "utm_source": rng.choice(["facebook", "google", "tiktok"]),
             "utm_medium": "cpc", "utm_campaign": rng.choice(["prospecting", "retargeting", "brand"])} if roll < 0.40
            else {"acquisition_domain": "pro." + state.shop_domain.split(".", 1)[1]} if roll < 0.60
            else {"is_created_by_admin": True} if roll < 0.66
            else {"acquisition_domain": state.shop_domain}
        )
        add_order(signed_up + timedelta(minutes=rng.randint(2, 50)), customer_id, lines,
                  is_subscription=subscription, **acquisition)

    for customer_id in state.rebuy_customers:
        if rng.random() < REBUY_PER_DAY and by_type["One-Time Purchase"]:
            add_order(_moment(rng, day), customer_id, [(rng.choice(by_type["One-Time Purchase"]), 1)],
                      acquisition_domain=state.shop_domain)

    if rng.random() < CART_PER_DAY and by_type["One-Time Purchase"]:
        add_order(_moment(rng, day), None, [(rng.choice(by_type["One-Time Purchase"]), 1)], kind="cart")

    pending.sort(key=lambda order: order[0])
    if len(pending) > len(block):
        raise PermanentError(f"{len(pending)} orders on {day} do not fit in its {len(block)} ids.")
    for order_id, (created_at, fields, lines) in zip(block, pending):
        cart = fields.pop("kind", None) == "cart"
        order_state = ("cart", "cart", "cart") if cart else _states(rng)
        for variant, quantity in lines:
            if len(plan.order_items) == len(block):
                raise PermanentError(f"More than {len(block)} order lines on {day}.")
            plan.order_items.append({
                "id": block[len(plan.order_items)], "order_id": order_id, "variant_id": variant.id,
                "quantity": quantity, "unit_price": variant.price, "units_total": quantity * variant.price,
                "adjustments_total": 0, "total": quantity * variant.price, "product_name": variant.product_name,
                "variant_name": variant.code, "updated_at": now,
            })
        items_total = sum(quantity * variant.price for variant, quantity in lines)
        adjustments = 0 if cart or rng.random() < 0.3 else state.shipping_cents
        plan.orders.append({
            "id": order_id, "number": f"{state.prefix}-{order_id:06d}", "created_from_order_id": None,
            "state": order_state[0], "payment_state": order_state[1], "shipping_state": order_state[2],
            "checkout_completed_at": None if cart else created_at, "items_total": items_total,
            "adjustments_total": adjustments, "total": items_total + adjustments, "currency_code": state.currency,
            "is_subscription": False, "is_sales_funnel": False, "is_created_by_admin": False, "acquisition_domain": None,
            "utm_source": None, "utm_medium": None, "utm_campaign": None,
            "created_at": created_at, "updated_at": now, **fields,
        })

    for order_id, number, total in state.refund_candidates:
        if rng.random() >= REFUND_RATE or len(plan.refund_payments) == len(block):
            continue
        full = rng.random() < 0.55
        completed = rng.random() < 0.90
        issued_at = _moment(rng, day)
        plan.refund_payments.append({
            "id": block[len(plan.refund_payments)], "order_number": number,
            "amount": total if full else max(1, total // 2), "currency_code": state.currency,
            "state": "completed" if completed else "cancelled",
            "refund_reason": rng.choice(["Damaged", "Changed mind", "Subscription Issue", "Late delivery"]),
            "requires_return": rng.random() < 0.4, "issued_at": issued_at, "updated_at": now,
        })
        if completed:
            plan.refunded_orders.append((order_id, "refunded" if full else "partially_refunded"))

    # Each campaign keeps spending about what it spent on an average day of the last four weeks
    for budget in state.budgets[:len(block)]:
        noise = rng.lognormvariate(0, 0.25)
        imported = datetime.combine(day, time(23, rng.randint(0, 59)))
        plan.marketing_spend.append({
            "id": block[len(plan.marketing_spend)], "spend_date": day, "utm_source": budget.utm_source,
            "utm_medium": budget.utm_medium, "utm_campaign": budget.utm_campaign,
            "amount": round(budget.amount * noise), "impressions": round(budget.impressions * noise),
            "clicks": round(budget.clicks * noise), "currency_code": state.currency,
            "created_at": imported, "updated_at": now,
        })

    plan.counts = {
        "new_customers": len(plan.customers),
        "renewals": sum(1 for o in plan.orders if o["created_from_order_id"]),
        "first_orders": sum(1 for o in plan.orders if o["customer_id"] in {c["id"] for c in plan.customers}),
        "orders": len(plan.orders),
        "cancellations": len(plan.cancellations),
        "refunds": len(plan.refund_payments),
        "ad_spend_cents": sum(row["amount"] for row in plan.marketing_spend),
    }
    return plan


def _one(cursor, sql: str, params: tuple | None = None):
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return row[0] if row else None


def load_state(cursor, day: date) -> DayState:
    start = datetime.combine(day, time())
    cursor.execute(
        "select v.id, p.name, v.code, v.price from product_variants v "
        "join products p on p.id = v.product_id where p.enabled order by v.id"
    )
    variants = [Variant(*row) for row in cursor.fetchall()]
    cursor.execute(
        """
        select s.id, s.customer_id,
               (select i.variant_id from order_items i where i.order_id = s.id order by i.id limit 1),
               coalesce((select max(r.created_at) from orders r
                         where r.created_from_order_id = s.id and r.created_at < %s), s.created_at),
               (select count(*) from orders r where r.created_from_order_id = s.id and r.created_at < %s)
        from orders s
        where s.is_subscription and s.payment_state in ('paid', 'partially_refunded', 'refunded')
          and s.created_at < %s and s.customer_id > %s
          and (not s.is_subscription_canceled or s.subscription_canceled_at >= %s)
        order by s.id
        """,
        (start, start, start, SCRIPTED_CUSTOMERS, start),
    )
    subscriptions = [Subscription(*row) for row in cursor.fetchall()]
    cursor.execute(
        """
        select o.customer_id
        from orders o
        where o.customer_id > %s and o.state <> 'cart' and o.created_at < %s
          and not exists (select 1 from orders s where s.customer_id = o.customer_id
                          and s.is_subscription and not s.is_subscription_canceled)
        group by o.customer_id
        having max(o.created_at) >= %s and max(o.created_at) < %s
        order by o.customer_id
        """,
        (SCRIPTED_CUSTOMERS, start, start - timedelta(days=90), start - timedelta(days=18)),
    )
    rebuy_customers = [row[0] for row in cursor.fetchall()]
    cursor.execute(
        """
        select o.id, o.number, o.total
        from orders o
        where o.payment_state = 'paid' and o.customer_id > %s
          and o.created_at >= %s and o.created_at < %s
          and not exists (select 1 from refund_payments r where r.order_number = o.number)
        order by o.id
        """,
        (SCRIPTED_CUSTOMERS, start - timedelta(days=7), start - timedelta(days=6)),
    )
    refund_candidates = [tuple(row) for row in cursor.fetchall()]
    recent_customers = _one(
        cursor, "select count(*) from customers where id > %s and created_at >= %s and created_at < %s",
        (SCRIPTED_CUSTOMERS, start - timedelta(days=SIGN_UP_RATE_DAYS), start),
    )
    cursor.execute(
        """
        select utm_source, utm_medium, utm_campaign, sum(amount) / 28.0, sum(impressions) / 28.0, sum(clicks) / 28.0
        from marketing_spend
        where spend_date >= %s and spend_date < %s
        group by utm_source, utm_medium, utm_campaign
        order by utm_source, utm_medium, utm_campaign
        """,
        (day - timedelta(days=28), day),
    )
    budgets = [Budget(source, medium, campaign, float(amount), float(impressions), float(clicks))
               for source, medium, campaign, amount, impressions, clicks in cursor.fetchall()]
    return DayState(
        prefix=_one(cursor, "select number from orders order by id limit 1").split("-")[0],
        currency=_one(cursor, "select currency_code from orders order by id limit 1"),
        country=_one(cursor, "select country_code from customers group by 1 order by count(*) desc limit 1"),
        shop_domain=_one(cursor, "select acquisition_domain from orders where acquisition_domain like 'shop.%' "
                                 "group by 1 order by count(*) desc limit 1"),
        shipping_cents=_one(cursor, "select max(adjustments_total) from orders") or 0,
        variants=variants,
        new_customers_per_day=max(0.5, recent_customers / SIGN_UP_RATE_DAYS) * 1.01,     # the shops keep growing a little
        subscriptions=subscriptions,
        rebuy_customers=rebuy_customers,
        refund_candidates=refund_candidates,
        budgets=budgets,
    )


def _insert(cursor, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    columns = list(rows[0])
    column_list = ", ".join(f'"{c}"' for c in columns)
    cursor.executemany(
        f'insert into "{table}" ({column_list}) values ({", ".join(["%s"] * len(columns))})',
        [tuple(row[c] for c in columns) for row in rows],
    )


def write_plan(cursor, plan: DayPlan, now: datetime) -> None:
    for table in TABLES:          # parents before children, for the foreign keys
        _insert(cursor, table, getattr(plan, table))
    for order_id, cancelled_at in plan.cancellations:
        cursor.execute(
            "update orders set is_subscription_canceled = true, subscription_canceled_at = %s, updated_at = %s where id = %s",
            (cancelled_at, now, order_id),
        )
    for order_id, payment_state in plan.refunded_orders:
        cursor.execute("update orders set payment_state = %s, updated_at = %s where id = %s",
                       (payment_state, now, order_id))


def simulate_day(brand_key: str, database: str, day: date) -> dict[str, int]:
    """Write the day's activity for one brand. Returns what was written (or that it already was)."""
    block = id_block(day)
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    with source_postgres.connect(database, conn_id=SOURCE_WRITER_CONN_ID) as conn, conn.cursor() as cursor:
        if _one(cursor, "select count(*) from orders where id between %s and %s", (block[0], block[-1])):
            log.info("%s %s is already in the CRM: nothing to do.", brand_key, day)
            return {"already_written": 1}
        if day > FIRST_DAY:
            # The replacement for Airflow's depends_on_past: a day starts from the state the
            # day before left. Every campaign spends every day, so an empty previous block means
            # that day was never written. A normal error, so a retry after the earlier day
            # lands can succeed.
            previous = id_block(day - timedelta(days=1))
            written = _one(
                cursor, "select count(*) from marketing_spend where id between %s and %s",
                (previous[0], previous[-1]),
            )
            if not written:
                raise RuntimeError(
                    f"{brand_key}: {day - timedelta(days=1)} is not in the CRM, so {day} cannot be written "
                    "in order. Run the earlier day first (a backfill fills them oldest first)."
                )
        later = _one(cursor, "select min(created_at) from orders where id > %s and id < %s", (block[-1], ID_END))
        if later:
            raise PermanentError(
                f"{brand_key}: a later day ({later:%Y-%m-%d}) is already written, so {day} can no longer be added "
                "in order. Reset the source database to start the simulated days again."
            )
        state = load_state(cursor, day)
        plan = plan_day(state, day, random.Random(f"{brand_key}:{day.isoformat()}"), now)
        write_plan(cursor, plan, now)
        conn.commit()
    ids = [o["id"] for o in plan.orders]
    log.info(
        "%s %s: %s new customers (%s first orders), %s renewals, %s cancellations, %s other orders, "
        "%s refunds, %s cents of ad spend; order ids %s",
        brand_key, day, plan.counts["new_customers"], plan.counts["first_orders"], plan.counts["renewals"],
        plan.counts["cancellations"], plan.counts["orders"] - plan.counts["renewals"] - plan.counts["first_orders"],
        plan.counts["refunds"], plan.counts["ad_spend_cents"], f"{ids[0]}-{ids[-1]}" if ids else "none",
    )
    return plan.counts
