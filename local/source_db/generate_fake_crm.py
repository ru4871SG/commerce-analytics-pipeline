#!/usr/bin/env python3
"""Generate local/source_db/fake_crm.sql: three fake CRM databases for the local demo.

The file is a psql script, deterministic (random.Random seeded per brand), and contains, for
each brand:

    CREATE DATABASE crm_<brand>, unless it exists (psql's \\gexec), then \\connect crm_<brand>
    DROP TABLE IF EXISTS ..., then CREATE TABLE ... (9 tables, 7 foreign keys)
    INSERT INTO ... (fake rows, parents before children)
    setval() on each id sequence, so new rows get the next free id

It needs only the Python standard library. Run it from the repository root:

    python local/source_db/generate_fake_crm.py

The data is fictional. Domains use the reserved .example TLD and customer emails use
example.com. Amounts are integer cents, as in the source application.
"""
from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path

SEED = 20260331
START = date(2025, 1, 1)          # the brands launch: first sign-ups and orders
END = date(2026, 9, 30)           # last day of data, the report's as-of date
OUT = Path(__file__).resolve().parent / "fake_crm.sql"

# Customers 1-8 of every brand are scripted edge cases (see build_brand). They sign up in the
# first quarter so their whole story fits inside the data.
SCRIPTED_CUSTOMERS = range(1, 9)
# First-item names that differ from the catalogue name, merged by
# dbt/seeds/product_name_corrections.csv: (brand, customer) -> (product code, name on the order).
NAME_VARIANTS = {
    ("broomx", 6): ("peak-whey", "Peak Whey Vanilla"),
    ("broomx", 8): ("night-recovery", "Night Recovery: Strawberry"),
    ("wecare", 6): ("dew-serum", "Dew Serum: NEW CUSTOMER OFFER"),
    ("greenz", 6): ("daily-greens", "Daily Greens Vanila"),
}

BRANDS = [
    {
        "key": "broomx",
        "name": "Broomx",
        "database": "crm_broomx",
        "currency": "USD",
        "country": "US",
        "domain": "shop.broomx.example",
        "pro_domain": "pro.broomx.example",
        "prefix": "SM",
        "customers": 860,
        "shipping_cents": 499,
        "products": [
            {"code": "peak-whey", "name": "Peak Whey", "price": 4999, "cost": "SU-WHEY-01", "vip": True, "upsell": False},
            {"code": "night-recovery", "name": "Night Recovery", "price": 3999, "cost": "SU-NIGHT-01", "vip": True, "upsell": True},
            {"code": "electro-hydrate", "name": "Electro Hydrate", "price": 2499, "cost": "SU-ELECTRO-01", "vip": True, "upsell": True},
            {"code": "daily-focus", "name": "Daily Focus", "price": 2999, "cost": None, "vip": False, "upsell": False},
        ],
        "costs": [
            ("SU-WHEY-01", 450, 1200, 600),
            ("SU-NIGHT-01", 420, 1000, 500),
            ("SU-ELECTRO-01", 300, 650, 300),
        ],
        "processing": (100, 45),
    },
    {
        "key": "wecare",
        "name": "WeCare",
        "database": "crm_wecare",
        "currency": "GBP",
        "country": "GB",
        "domain": "shop.wecare.example",
        "pro_domain": "pro.wecare.example",
        "prefix": "LU",
        "customers": 790,
        "shipping_cents": 399,
        "products": [
            {"code": "dew-serum", "name": "Dew Serum", "price": 3499, "cost": "LU-DEW-01", "vip": True, "upsell": True},
            {"code": "barrier-balm", "name": "Barrier Balm", "price": 2799, "cost": "LU-BARRIER-01", "vip": True, "upsell": False},
            {"code": "bright-c", "name": "Bright C", "price": 3199, "cost": "LU-BRIGHT-01", "vip": True, "upsell": True},
            {"code": "night-cream", "name": "Night Cream", "price": 3899, "cost": None, "vip": False, "upsell": False},
        ],
        "costs": [
            ("LU-DEW-01", 350, 900, 450),
            ("LU-BARRIER-01", 330, 800, 400),
            ("LU-BRIGHT-01", 360, 950, 480),
        ],
        "processing": (95, 40),
    },
    {
        "key": "greenz",
        "name": "Greenz",
        "database": "crm_greenz",
        "currency": "GBP",
        "country": "GB",
        "domain": "shop.greenz.example",
        "pro_domain": "pro.greenz.example",
        "prefix": "VD",
        "customers": 830,
        "shipping_cents": 399,
        "products": [
            {"code": "daily-greens", "name": "Daily Greens", "price": 4499, "cost": "VD-GREENS-01", "vip": True, "upsell": True},
            {"code": "gut-reset", "name": "Gut Reset", "price": 3999, "cost": "VD-GUT-01", "vip": True, "upsell": False},
            {"code": "berry-boost", "name": "Berry Boost", "price": 2999, "cost": "VD-BERRY-01", "vip": True, "upsell": True},
            {"code": "clean-protein", "name": "Clean Protein", "price": 5499, "cost": None, "vip": False, "upsell": False},
        ],
        "costs": [
            ("VD-GREENS-01", 400, 1050, 520),
            ("VD-GUT-01", 380, 950, 470),
            ("VD-BERRY-01", 320, 750, 350),
        ],
        "processing": (105, 50),
    },
]

TABLE_ORDER = [
    "customers",
    "product_costs",
    "processing_costs",
    "products",
    "product_variants",
    "orders",
    "order_items",
    "refund_payments",
    "marketing_spend",
]

# Paid channels (the utm_source of a paid first order): what a new customer should cost there,
# in cents of the brand's currency, the cost per 1,000 impressions and the click-through rate.
# Each day's budget follows the channel's paid sign-ups, so CAC comes out near the target.
CHANNELS = {
    "facebook": (1500, 900, 0.012),
    "google": (2200, 1400, 0.035),
    "tiktok": (3600, 600, 0.009),
}
CAMPAIGNS = {"prospecting": 0.60, "retargeting": 0.25, "brand": 0.15}   # share of a channel's budget

# Flags are BOOLEAN. PostgreSQL does not index a foreign key by itself, so each one has a
# CREATE INDEX. The five tables the pipeline loads incrementally are read by updated_at
# (crm_pipeline/source_postgres.py iter_rows), so updated_at is indexed too: without it every
# daily load scans the whole table.
DDL = {
    "customers": """
CREATE TABLE customers (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
    email VARCHAR(255) NOT NULL,
    country_code CHAR(2) NOT NULL,
    subscribed_to_newsletter BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_customers_email UNIQUE (email)
);
CREATE INDEX idx_customers_updated_at ON customers (updated_at)""",
    "product_costs": """
CREATE TABLE product_costs (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
    sku VARCHAR(64) NOT NULL,
    shipping_amount INTEGER NOT NULL,
    first_amount INTEGER NOT NULL,
    addition_amount INTEGER NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_product_costs_sku UNIQUE (sku)
)""",
    "processing_costs": """
CREATE TABLE processing_costs (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
    first_item_amount INTEGER NOT NULL,
    additional_item_amount INTEGER NOT NULL,
    PRIMARY KEY (id)
)""",
    "products": """
CREATE TABLE products (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
    code VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    sku VARCHAR(64) NULL,
    mint_soft_sku VARCHAR(64) NULL,
    mint_soft_name VARCHAR(255) NULL,
    product_cost_sku VARCHAR(64) NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_products_code UNIQUE (code),
    CONSTRAINT fk_products_cost FOREIGN KEY (product_cost_sku) REFERENCES product_costs (sku)
);
CREATE INDEX idx_products_cost_sku ON products (product_cost_sku)""",
    "product_variants": """
CREATE TABLE product_variants (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
    product_id INTEGER NOT NULL,
    code VARCHAR(255) NOT NULL,
    position INTEGER NOT NULL DEFAULT 0,
    on_hand INTEGER NOT NULL DEFAULT 0,
    tracked BOOLEAN NOT NULL DEFAULT FALSE,
    shipping_required BOOLEAN NOT NULL DEFAULT TRUE,
    price INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_product_variants_code UNIQUE (code),
    CONSTRAINT fk_variants_product FOREIGN KEY (product_id) REFERENCES products (id)
);
CREATE INDEX idx_product_variants_product ON product_variants (product_id)""",
    "orders": """
CREATE TABLE orders (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
    number VARCHAR(32) NOT NULL,
    customer_id INTEGER NULL,
    created_from_order_id INTEGER NULL,
    state VARCHAR(32) NOT NULL,
    payment_state VARCHAR(32) NULL,
    shipping_state VARCHAR(32) NULL,
    checkout_completed_at TIMESTAMP NULL,
    items_total INTEGER NOT NULL,
    adjustments_total INTEGER NOT NULL,
    total INTEGER NOT NULL,
    currency_code CHAR(3) NOT NULL,
    is_subscription BOOLEAN NOT NULL DEFAULT FALSE,
    is_subscription_canceled BOOLEAN NOT NULL DEFAULT FALSE,
    subscription_canceled_at TIMESTAMP NULL,
    subscription_paused BOOLEAN NOT NULL DEFAULT FALSE,
    is_created_by_admin BOOLEAN NOT NULL DEFAULT FALSE,
    is_sales_funnel BOOLEAN NOT NULL DEFAULT FALSE,
    acquisition_domain VARCHAR(255) NULL,
    utm_source VARCHAR(255) NULL,
    utm_medium VARCHAR(255) NULL,
    utm_campaign VARCHAR(255) NULL,
    utm_content VARCHAR(255) NULL,
    utm_term VARCHAR(255) NULL,
    utm_id VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_orders_number UNIQUE (number),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers (id),
    CONSTRAINT fk_orders_renewal FOREIGN KEY (created_from_order_id) REFERENCES orders (id)
);
CREATE INDEX idx_orders_customer ON orders (customer_id);
CREATE INDEX idx_orders_created_from ON orders (created_from_order_id);
CREATE INDEX idx_orders_created_at ON orders (created_at);
CREATE INDEX idx_orders_updated_at ON orders (updated_at)""",
    "order_items": """
CREATE TABLE order_items (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
    order_id INTEGER NOT NULL,
    variant_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price INTEGER NOT NULL,
    units_total INTEGER NOT NULL,
    adjustments_total INTEGER NOT NULL DEFAULT 0,
    total INTEGER NOT NULL,
    product_name VARCHAR(255) NULL,
    variant_name VARCHAR(255) NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
    CONSTRAINT fk_items_variant FOREIGN KEY (variant_id) REFERENCES product_variants (id)
);
CREATE INDEX idx_order_items_order ON order_items (order_id);
CREATE INDEX idx_order_items_variant ON order_items (variant_id);
CREATE INDEX idx_order_items_updated_at ON order_items (updated_at)""",
    "refund_payments": """
CREATE TABLE refund_payments (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
    order_number VARCHAR(32) NOT NULL,
    amount INTEGER NOT NULL,
    currency_code CHAR(3) NOT NULL,
    state VARCHAR(32) NOT NULL,
    refund_reason VARCHAR(255) NULL,
    requires_return BOOLEAN NOT NULL DEFAULT FALSE,
    issued_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_refunds_order FOREIGN KEY (order_number) REFERENCES orders (number)
);
CREATE INDEX idx_refund_payments_order_number ON refund_payments (order_number);
CREATE INDEX idx_refund_payments_updated_at ON refund_payments (updated_at)""",
    "marketing_spend": """
CREATE TABLE marketing_spend (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
    spend_date DATE NOT NULL,
    utm_source VARCHAR(255) NOT NULL,
    utm_medium VARCHAR(255) NOT NULL,
    utm_campaign VARCHAR(255) NOT NULL,
    amount INTEGER NOT NULL,
    impressions INTEGER NOT NULL,
    clicks INTEGER NOT NULL,
    currency_code CHAR(3) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_marketing_spend_day UNIQUE (spend_date, utm_source, utm_medium, utm_campaign)
);
CREATE INDEX idx_marketing_spend_updated_at ON marketing_spend (updated_at)""",
}


@dataclass
class Variant:
    id: int
    product_id: int
    code: str
    purchase_type: str
    price: int
    product_name: str


@dataclass
class Order:
    created_at: datetime
    customer_id: int | None
    kind: str
    items: list[dict] = field(default_factory=list)
    state: str = "fulfilled"
    payment_state: str | None = "paid"
    shipping_state: str | None = "shipped"
    checkout_completed_at: datetime | None = None
    adjustments_total: int = 0
    is_subscription: bool = False
    is_subscription_canceled: bool = False
    subscription_canceled_at: datetime | None = None
    subscription_paused: bool = False
    is_created_by_admin: bool = False
    is_sales_funnel: bool = False
    acquisition_domain: str | None = None
    utm_source: str | None = None
    utm_medium: str | None = None
    utm_campaign: str | None = None
    utm_content: str | None = None
    utm_term: str | None = None
    utm_id: str | None = None
    parent: "Order | None" = None
    updated_at: datetime | None = None
    id: int | None = None
    number: str | None = None

    @property
    def items_total(self) -> int:
        return sum(item["total"] for item in self.items)

    @property
    def total(self) -> int:
        return self.items_total + self.adjustments_total


def sql_value(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return "'" + value.strftime("%Y-%m-%d %H:%M:%S") + "'"
    if isinstance(value, date):
        return "'" + value.isoformat() + "'"
    return "'" + str(value).replace("'", "''") + "'"      # a backslash is literal in PostgreSQL


def render_inserts(table: str, columns: list[str], rows: list[tuple], batch_size: int = 500) -> str:
    if not rows:
        return ""
    out = []
    column_list = ", ".join(columns)
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        values = ",\n".join("(" + ", ".join(sql_value(v) for v in row) + ")" for row in batch)
        out.append(f"INSERT INTO {table} ({column_list}) VALUES\n{values};")
    return "\n".join(out) + "\n"


def random_datetime(rng: random.Random, start: date, end: date) -> datetime:
    span = (end - start).days
    day = start + timedelta(days=rng.randint(0, max(span, 0)))
    return datetime.combine(day, time(rng.randint(7, 21), rng.randint(0, 59), rng.randint(0, 59)))


def signup_datetime(rng: random.Random) -> datetime:
    """A sign-up between launch and the last days of data. The business grows: the daily
    sign-up rate rises by about 85% from launch to the end."""
    span = (END - START).days - 3
    offset = rng.uniform(0, span) if rng.random() < 0.70 else rng.triangular(0, span, span)
    day = START + timedelta(days=int(offset))
    return datetime.combine(day, time(rng.randint(7, 21), rng.randint(0, 59), rng.randint(0, 59)))


def choose_state(rng: random.Random, when: datetime, kind: str, special_cancelled: bool = False) -> tuple[str, str, str]:
    """Return (state, payment_state, shipping_state) for a non-cart order."""
    if special_cancelled:
        return "cancelled", "cancelled", "cancelled"
    if kind == "renewal" and rng.random() < 0.012:
        return "cancelled", "cancelled", "cancelled"
    if rng.random() < 0.03:
        return "cancelled", "cancelled", "cancelled"
    if rng.random() < 0.01:
        return "new", "awaiting_payment", "ready"
    if when.date() > END - timedelta(days=9):
        return "new", "paid", "ready"
    return "fulfilled", "paid", "shipped"


def make_item(rng: random.Random, variant: Variant, quantity: int | None = None,
              name_override: str | None = None) -> dict:
    quantity = quantity or (1 if rng.random() < 0.80 else 2 if rng.random() < 0.80 else 3)
    units_total = quantity * variant.price
    adjustments = -round(units_total * 0.10) if rng.random() < 0.08 else 0
    return {
        "variant": variant,
        "quantity": quantity,
        "unit_price": variant.price,
        "units_total": units_total,
        "adjustments_total": adjustments,
        "total": units_total + adjustments,
        "product_name": name_override if name_override is not None else variant.product_name,
        "variant_name": variant.code,
    }


def apply_acquisition(rng: random.Random, order: Order, brand: dict) -> None:
    roll = rng.random()
    if roll < 0.40:
        order.is_sales_funnel = True
        order.acquisition_domain = brand["domain"]
        order.utm_source = rng.choice(["facebook", "google", "tiktok"])
        order.utm_medium = "cpc"
        order.utm_campaign = rng.choice(["prospecting", "retargeting", "brand"])
        order.utm_content = rng.choice(["video-a", "video-b", "static-1"])
        order.utm_term = rng.choice(["supplement", "wellness", "offer"])
        order.utm_id = f"{brand['key'][:2]}-{rng.randint(10000, 99999)}"
    elif roll < 0.60:
        order.acquisition_domain = brand["pro_domain"]
    elif roll < 0.66:
        order.is_created_by_admin = True
    else:
        order.acquisition_domain = brand["domain"]


def build_brand(brand: dict, brand_index: int) -> dict[str, list[tuple]]:
    rng = random.Random(SEED + brand_index * 10_007)
    rows: dict[str, list[tuple]] = defaultdict(list)

    # Products and variants -------------------------------------------------
    products = []
    for product_id, product in enumerate(brand["products"], start=1):
        created = random_datetime(rng, date(2024, 5, 1), date(2024, 12, 20))
        products.append({
            **product,
            "id": product_id,
            "created_at": created,
            "updated_at": created + timedelta(days=rng.randint(1, 40)),
        })
        rows["products"].append((
            product_id,
            product["code"],
            product["name"],
            True,
            f"{brand['key'][:2].upper()}-{product['code'].upper()}",
            f"MINT-{brand['key'][:2].upper()}-{product['code'].upper()}",
            product["name"],
            product["cost"],
            created,
            created + timedelta(days=rng.randint(1, 40)),
        ))

    variants: list[Variant] = []
    variant_id = 1
    for product in products:
        variant_defs = [("1", "One-Time Purchase", product["price"])]
        if product["vip"]:
            variant_defs.append(("vip", "Subscribe", product["price"]))
        if product["upsell"]:
            variant_defs.append(("u1", "Upsell", max(500, product["price"] - 700)))
        for suffix, purchase_type, price in variant_defs:
            code = f"{product['code']}-{suffix}"
            created = product["created_at"]
            rows["product_variants"].append((
                variant_id,
                product["id"],
                code,
                len(variants),
                100 + rng.randint(0, 900),
                suffix != "vip",
                True,
                price,
                created,
                created + timedelta(days=rng.randint(0, 5)),
            ))
            variants.append(Variant(variant_id, product["id"], code, purchase_type, price, product["name"]))
            variant_id += 1

    rows["product_costs"] = [
        (i, sku, shipping, first, addition)
        for i, (sku, shipping, first, addition) in enumerate(brand["costs"], start=1)
    ]
    rows["processing_costs"] = [(1, brand["processing"][0], brand["processing"][1])]

    upsell = [v for v in variants if v.purchase_type == "Upsell"]

    # Customers -------------------------------------------------------------
    customer_count = brand["customers"]
    customers = []
    for customer_id in range(1, customer_count + 1):
        if customer_id in SCRIPTED_CUSTOMERS:
            signed_up = random_datetime(rng, date(2025, 1, 2), date(2025, 3, 31))
        else:
            signed_up = signup_datetime(rng)
        country = brand["country"]
        if rng.random() < 0.04:
            country = rng.choice(["IE", "US", "CA"])
        customers.append({
            "id": customer_id,
            "signed_up": signed_up,
            "country": country,
            "newsletter": rng.random() < 0.55,
        })
        rows["customers"].append((
            customer_id,
            f"customer{customer_id}@example.com",
            country,
            customers[-1]["newsletter"],
            signed_up,
            signed_up + timedelta(days=rng.randint(0, 10)),
        ))

    # First orders ----------------------------------------------------------
    orders: list[Order] = []
    first_orders: dict[int, Order] = {}
    product_pick = products
    vip_products = [p for p in products if p["vip"]]

    for customer in customers:
        customer_id = customer["id"]
        scripted = customer_id in SCRIPTED_CUSTOMERS
        # A few customers never place an order: they still exist in the CRM.
        if not scripted and rng.random() < 0.04:
            continue
        signed_up = customer["signed_up"]
        created = signed_up + timedelta(days=rng.randint(0, 21), hours=rng.randint(0, 12))
        if created.date() > END:
            continue

        # Scripted edge cases: 1 and 4 are test accounts, 2's first order is cancelled and its
        # second is paid, 3 cancels exactly 60 days after starting, 5 is cancelled with no date,
        # 6 and 8 order under a name the corrections seed merges, 7's line has no product name.
        if customer_id in (3, 5):
            subscription = True
        else:
            subscription = rng.random() < 0.50 and customer_id not in (1, 2, 4)
        variant_name = NAME_VARIANTS.get((brand["key"], customer_id))
        if variant_name:
            product = next(p for p in products if p["code"] == variant_name[0])
        else:
            product = rng.choice(vip_products if subscription else product_pick)
        variants_for_product = [v for v in variants if v.product_id == product["id"]]
        primary = next(v for v in variants_for_product if v.purchase_type == ("Subscribe" if subscription else "One-Time Purchase"))
        items = [make_item(rng, primary)]

        # 15% of first orders add an upsell line.
        if upsell and primary.purchase_type != "Upsell" and rng.random() < 0.15:
            items.append(make_item(rng, rng.choice(upsell), quantity=1))

        if variant_name:
            items[0]["product_name"] = variant_name[1]
        if customer_id == 7:
            items[0]["product_name"] = ""

        if customer_id == 2:
            state, payment_state, shipping_state = choose_state(rng, created, "first", special_cancelled=True)
        elif scripted:
            state, payment_state, shipping_state = "fulfilled", "paid", "shipped"
        else:
            state, payment_state, shipping_state = choose_state(rng, created, "first")
        order = Order(
            created_at=created,
            customer_id=customer_id,
            kind="first",
            items=items,
            state=state,
            payment_state=payment_state,
            shipping_state=shipping_state,
            checkout_completed_at=created,
            adjustments_total=0,
            is_subscription=subscription,
        )
        order.adjustments_total = 0 if rng.random() < 0.30 else brand["shipping_cents"]
        if rng.random() < 0.12:
            order.adjustments_total -= 500
        apply_acquisition(rng, order, brand)
        orders.append(order)
        first_orders[customer_id] = order

    # Subscriptions: renewals and cancellation -------------------------------
    for order in orders:
        customer_id = order.customer_id
        # A subscription starts with a paid first order: one never paid does not renew
        if not order.is_subscription or customer_id is None or order.payment_state != "paid":
            continue

        start = order.created_at
        if customer_id == 3:
            cancel_at = start + timedelta(days=60)
            special_cancel = "exact60"
        elif customer_id == 5:
            cancel_at = None
            special_cancel = "null"
        else:
            cancel_at = None
            special_cancel = None

        next_at = start + timedelta(days=rng.randint(28, 32))
        renewal_count = 0
        cancelled = False
        while next_at.date() <= END:
            if special_cancel == "exact60" and next_at >= cancel_at:
                break
            if special_cancel == "null" and next_at > start + timedelta(days=90):
                break
            if special_cancel is None:
                churn_probability = 0.20 if renewal_count < 2 else 0.08
                if rng.random() < churn_probability:
                    cancel_at = next_at - timedelta(days=rng.randint(0, 3))
                    cancelled = True
                    break

            state, payment_state, shipping_state = choose_state(rng, next_at, "renewal")
            renewal = Order(
                created_at=next_at,
                customer_id=customer_id,
                kind="renewal",
                items=[make_item(rng, next(v for v in variants if v.code == order.items[0]["variant"].code), quantity=1)],
                state=state,
                payment_state=payment_state,
                shipping_state=shipping_state,
                checkout_completed_at=next_at,
                adjustments_total=0,
                parent=order,
            )
            orders.append(renewal)
            renewal_count += 1
            next_at = next_at + timedelta(days=rng.randint(28, 32))

        if special_cancel == "exact60":
            order.is_subscription_canceled = True
            order.subscription_canceled_at = cancel_at
        elif special_cancel == "null":
            order.is_subscription_canceled = True
            order.subscription_canceled_at = None
        elif cancelled:
            order.is_subscription_canceled = True
            order.subscription_canceled_at = cancel_at

    # A few active subscriptions are paused (they keep renewing).
    active_subs = [
        o for o in orders
        if o.kind == "first" and o.is_subscription and not o.is_subscription_canceled
        and o.payment_state == "paid" and o.created_at.date() <= END - timedelta(days=120)
    ]
    for order in active_subs[:max(2, len(active_subs) // 80)]:
        order.subscription_paused = True

    # One-time customers: a subset orders again. Customer 2 is forced to have a
    # cancelled first non-cart order followed by a paid order.
    for customer_id, first in list(first_orders.items()):
        if first.is_subscription:
            continue
        extra_count = 0
        if customer_id == 2:
            extra_count = 1
        elif customer_id in (1, 4):
            extra_count = 1
        elif rng.random() < 0.42:
            extra_count = 1 + (1 if rng.random() < 0.30 else 0) + (1 if rng.random() < 0.10 else 0)

        when = first.created_at + timedelta(days=rng.randint(18, 60))
        for _ in range(extra_count):
            if when.date() > END:
                break
            product = rng.choice(products)
            variant = next(v for v in variants if v.product_id == product["id"] and v.purchase_type == "One-Time Purchase")
            if customer_id in SCRIPTED_CUSTOMERS:
                state, payment_state, shipping_state = "fulfilled", "paid", "shipped"
            else:
                state, payment_state, shipping_state = choose_state(rng, when, "rebuy")
            rebuy = Order(
                created_at=when,
                customer_id=customer_id,
                kind="rebuy",
                items=[make_item(rng, variant)],
                state=state,
                payment_state=payment_state,
                shipping_state=shipping_state,
                checkout_completed_at=when,
                adjustments_total=0 if rng.random() < 0.4 else brand["shipping_cents"],
            )
            orders.append(rebuy)
            when = when + timedelta(days=rng.randint(35, 150))

    # Carts (5% as many as real orders), some without a customer.
    cart_count = max(1, int(len(orders) * 0.05))
    for _ in range(cart_count):
        when = random_datetime(rng, START, END)
        customer = rng.choice(customers) if rng.random() < 0.80 else None
        product = rng.choice(products)
        variant = next(v for v in variants if v.product_id == product["id"] and v.purchase_type == "One-Time Purchase")
        orders.append(Order(
            created_at=when,
            customer_id=customer["id"] if customer else None,
            kind="cart",
            items=[make_item(rng, variant)],
            state="cart",
            payment_state="cart",
            shipping_state="cart",
            checkout_completed_at=None,
            adjustments_total=0,
        ))

    # Stable ids: first orders before their renewals on the same timestamp.
    kind_priority = {"first": 0, "rebuy": 1, "renewal": 2, "cart": 3}
    orders.sort(key=lambda o: (o.created_at, kind_priority[o.kind]))
    for order_id, order in enumerate(orders, start=1):
        order.id = order_id
        order.number = f"{brand['prefix']}-{order_id:06d}"

    # Order and item rows ---------------------------------------------------
    item_id = 1
    refund_id = 1
    for order in orders:
        order.updated_at = order.created_at + timedelta(minutes=rng.randint(5, 120))
        for item in order.items:
            rows["order_items"].append((
                item_id,
                order.id,
                item["variant"].id,
                item["quantity"],
                item["unit_price"],
                item["units_total"],
                item["adjustments_total"],
                item["total"],
                item["product_name"],
                item["variant_name"],
                order.updated_at,
            ))
            item_id += 1

        rows["orders"].append((
            order.id,
            order.number,
            order.customer_id,
            order.parent.id if order.parent else None,
            order.state,
            order.payment_state,
            order.shipping_state,
            order.checkout_completed_at,
            order.items_total,
            order.adjustments_total,
            order.total,
            brand["currency"],
            order.is_subscription,
            order.is_subscription_canceled,
            order.subscription_canceled_at,
            order.subscription_paused,
            order.is_created_by_admin,
            order.is_sales_funnel,
            order.acquisition_domain,
            order.utm_source,
            order.utm_medium,
            order.utm_campaign,
            order.utm_content,
            order.utm_term,
            order.utm_id,
            order.created_at,
            order.updated_at,
        ))

    # Refunds: ~5% of paid orders, mostly completed, a few cancelled rows.
    for order in orders:
        if order.payment_state not in ("paid", "partially_refunded"):
            continue
        if order.kind == "cart" or rng.random() >= 0.05:
            continue
        full = rng.random() < 0.55
        amount = order.total if full else max(1, order.total // 2)
        issued_at = min(
            order.created_at + timedelta(days=rng.randint(1, 30), hours=rng.randint(0, 8)),
            datetime.combine(END, time(23, 59, 59)),
        )
        state = "completed" if rng.random() < 0.90 else "cancelled"
        rows["refund_payments"].append((
            refund_id,
            order.number,
            amount,
            brand["currency"],
            state,
            rng.choice(["Damaged", "Changed mind", "Subscription Issue", "Late delivery"]),
            rng.random() < 0.4,
            issued_at,
            issued_at,
        ))
        refund_id += 1
        if state == "completed":
            order.payment_state = "refunded" if full else "partially_refunded"
            order.updated_at = max(order.updated_at, issued_at)

    # The order rows were written before refunds changed payment_state; fix those rows.
    by_id = {o.id: o for o in orders}
    for index, row in enumerate(rows["orders"]):
        order = by_id[row[0]]
        updated = list(row)
        updated[5] = order.payment_state
        updated[-1] = order.updated_at
        rows["orders"][index] = tuple(updated)

    # Ad spend: one row per day, channel and campaign from the brand's first order. It has its
    # own random stream, so adding it changed none of the rows above.
    spend_rng = random.Random(SEED + brand_index * 10_007 + 1)
    paid_first_orders = defaultdict(int)          # (day, utm_source) -> new paid customers
    for order in orders:
        if order.kind == "first" and order.is_sales_funnel:
            paid_first_orders[(order.created_at.date(), order.utm_source)] += 1
    day = min(o.created_at.date() for o in orders if o.kind == "first")
    while day <= END:
        for source, (cac, cpm, ctr) in CHANNELS.items():
            # The channel's paid sign-ups per day over the four weeks around this one
            recent = sum(paid_first_orders[(day + timedelta(days=d), source)] for d in range(-14, 14)) / 28
            budget = max(recent, 0.05) * cac * spend_rng.lognormvariate(0, 0.25)
            for campaign, share in CAMPAIGNS.items():
                amount = round(budget * share * spend_rng.uniform(0.8, 1.2))
                impressions = round(amount / cpm * 1000 * spend_rng.uniform(0.85, 1.15))
                clicks = round(impressions * ctr * spend_rng.uniform(0.8, 1.2))
                imported = datetime.combine(day, time(23, spend_rng.randint(0, 59)))
                rows["marketing_spend"].append((
                    len(rows["marketing_spend"]) + 1, day, source, "cpc", campaign, amount,
                    impressions, clicks, brand["currency"], imported, imported,
                ))
        day += timedelta(days=1)

    summary = {
        "customers": len(rows["customers"]),
        "products": len(rows["products"]),
        "variants": len(rows["product_variants"]),
        "orders": len(rows["orders"]),
        "non_cart_orders": sum(1 for o in orders if o.state != "cart"),
        "paid_orders": sum(1 for o in orders if o.payment_state in ("paid", "partially_refunded", "refunded")),
        "paid_revenue_cents": sum(o.total for o in orders if o.payment_state in ("paid", "partially_refunded", "refunded")),
        "refunds": len(rows["refund_payments"]),
        "ad_spend_cents": sum(row[5] for row in rows["marketing_spend"]),
    }
    return dict(rows), summary


INSERT_COLUMNS = {
    "customers": ["id", "email", "country_code", "subscribed_to_newsletter", "created_at", "updated_at"],
    "product_costs": ["id", "sku", "shipping_amount", "first_amount", "addition_amount"],
    "processing_costs": ["id", "first_item_amount", "additional_item_amount"],
    "products": ["id", "code", "name", "enabled", "sku", "mint_soft_sku", "mint_soft_name",
                 "product_cost_sku", "created_at", "updated_at"],
    "product_variants": ["id", "product_id", "code", "position", "on_hand", "tracked",
                         "shipping_required", "price", "created_at", "updated_at"],
    "orders": ["id", "number", "customer_id", "created_from_order_id", "state", "payment_state",
               "shipping_state", "checkout_completed_at", "items_total", "adjustments_total",
               "total", "currency_code", "is_subscription", "is_subscription_canceled",
               "subscription_canceled_at", "subscription_paused", "is_created_by_admin",
               "is_sales_funnel", "acquisition_domain", "utm_source", "utm_medium",
               "utm_campaign", "utm_content", "utm_term", "utm_id", "created_at", "updated_at"],
    "order_items": ["id", "order_id", "variant_id", "quantity", "unit_price", "units_total",
                    "adjustments_total", "total", "product_name", "variant_name", "updated_at"],
    "refund_payments": ["id", "order_number", "amount", "currency_code", "state",
                        "refund_reason", "requires_return", "issued_at", "updated_at"],
    "marketing_spend": ["id", "spend_date", "utm_source", "utm_medium", "utm_campaign", "amount",
                        "impressions", "clicks", "currency_code", "created_at", "updated_at"],
}


def main() -> None:
    parts = [
        "-- Generated by local/source_db/generate_fake_crm.py. Do not edit by hand.\n"
        "-- Three fictional CRM databases. Amounts are integer cents; dates are UTC.\n"
        "-- A psql script (\\gexec, \\connect): run it with psql, as a superuser.\n",
    ]
    summaries = []
    for index, brand in enumerate(BRANDS):
        rows, summary = build_brand(brand, index)
        summaries.append((brand, summary))
        parts.append(
            f"\n-- =====================================================================\n"
            f"-- {brand['name']} ({brand['database']})\n"
            f"-- =====================================================================\n"
            f"SELECT 'CREATE DATABASE {brand['database']} ENCODING ''UTF8''' "
            f"WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '{brand['database']}')\\gexec\n"
            f"\\connect {brand['database']}\n"
            # Children before parents, for the foreign keys
            f"DROP TABLE IF EXISTS {', '.join(reversed(TABLE_ORDER))};\n"
        )
        for table in TABLE_ORDER:
            parts.append(DDL[table].strip() + ";\n")
        for table in TABLE_ORDER:
            inserts = render_inserts(table, INSERT_COLUMNS[table], rows.get(table, []))
            if inserts:
                parts.append(inserts)
        # Explicit ids do not move an identity sequence on
        parts.append("".join(
            f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), max(id)) FROM {table};\n"
            for table in TABLE_ORDER
        ))
    OUT.write_text("\n".join(parts))

    print(f"Wrote {OUT} ({OUT.stat().st_size / 1_000_000:.2f} MB)")
    print(f"{'brand':8} {'customers':>9} {'products':>8} {'variants':>8} {'orders':>8} "
          f"{'non-cart':>9} {'paid':>7} {'revenue':>13} {'refunds':>7} {'ad spend':>11}")
    total_orders = 0
    total_revenue = 0
    for brand, s in summaries:
        total_orders += s["orders"]
        total_revenue += s["paid_revenue_cents"]
        print(f"{brand['key']:8} {s['customers']:9,} {s['products']:8,} {s['variants']:8,} "
              f"{s['orders']:8,} {s['non_cart_orders']:9,} {s['paid_orders']:7,} "
              f"{s['paid_revenue_cents']:13,} {s['refunds']:7,} {s['ad_spend_cents']:11,}")
    print(f"{'TOTAL':8} {'':9} {'':8} {'':8} {total_orders:8,} {'':9} {'':7} {total_revenue:13,}")


if __name__ == "__main__":
    main()
