"""The activity simulator's day plan. plan_day is pure, so no database is needed."""
import random
from datetime import date, datetime, timedelta, timezone

import pytest

from crm_pipeline import simulator
from crm_pipeline.errors import PermanentError
from crm_pipeline.simulator import Budget, DayState, Subscription, Variant

DAY = date(2026, 10, 21)
NOW = datetime(2026, 10, 22, 11, 0)


def test_the_morning_run_records_the_day_before_in_new_york():
    # 07:00 EDT on 22 October is 11:00 UTC
    assert simulator.business_day(datetime(2026, 10, 22, 11, 0, tzinfo=timezone.utc)) == date(2026, 10, 21)


def test_every_day_owns_fifty_ids_inside_the_range():
    assert simulator.id_block(date(2026, 10, 1)) == range(20_000, 20_050)
    assert simulator.id_block(date(2026, 10, 1) + timedelta(days=999)) == range(69_950, 70_000)
    with pytest.raises(PermanentError):
        simulator.id_block(date(2026, 10, 1) + timedelta(days=1000))
    with pytest.raises(PermanentError):
        simulator.id_block(date(2026, 9, 30))       # the seed data covers it


BUDGETS = [Budget(source, "cpc", campaign, 500.0, 50_000.0, 600.0)
           for source in ("facebook", "google", "tiktok") for campaign in ("brand", "prospecting", "retargeting")]


def state(subscriptions, refund_candidates=(), budgets=BUDGETS):
    return DayState(
        prefix="SM", currency="USD", country="US", shop_domain="shop.broomx.example", shipping_cents=499,
        variants=[Variant(1, "Peak Whey", "peak-whey-1", 4999), Variant(2, "Peak Whey", "peak-whey-vip", 4999),
                  Variant(3, "Peak Whey", "peak-whey-u1", 4299)],
        new_customers_per_day=3.0, subscriptions=subscriptions, rebuy_customers=list(range(100, 200)),
        refund_candidates=list(refund_candidates), budgets=list(budgets),
    )


def plan(day_state, seed="broomx:2026-10-21"):
    return simulator.plan_day(day_state, DAY, random.Random(seed), NOW)


def test_only_subscriptions_that_are_due_renew_or_cancel():
    due = Subscription(order_id=500, customer_id=50, variant_id=2, last_order_at=datetime(2026, 9, 10, 9), renewals=0)
    not_due = Subscription(order_id=501, customer_id=51, variant_id=2, last_order_at=datetime(2026, 10, 15, 9), renewals=0)
    day = plan(state([due, not_due]))
    renewed = {o["created_from_order_id"] for o in day.orders if o["created_from_order_id"]}
    cancelled = {order_id for order_id, _ in day.cancellations}
    assert renewed | cancelled == {500}
    assert not renewed & cancelled


def _share_cancelled(renewals):
    due = [Subscription(500 + i, 50 + i, 2, datetime(2026, 9, 10, 9), renewals) for i in range(30)]
    cancelled = sum(len(plan(state(due), seed=f"broomx:{i}").cancellations) for i in range(100))
    return cancelled / (30 * 100)


def test_the_first_two_renewals_cancel_more_often_than_later_ones_as_in_the_seed():
    assert 0.16 < _share_cancelled(renewals=0) < 0.24
    assert 0.16 < _share_cancelled(renewals=1) < 0.24
    assert 0.05 < _share_cancelled(renewals=2) < 0.11


class _StateCursor:
    """Answers load_state's queries, and holds each to as many parameters as it has placeholders."""

    SUBSCRIPTIONS = [(500, 50, 2, datetime(2026, 9, 10, 9), 3)]

    def __init__(self):
        self.queries = []

    def execute(self, sql, params=None):
        assert sql.count("%s") == len(params or ())
        self.queries.append(sql)

    def fetchall(self):
        return self.SUBSCRIPTIONS if "s.is_subscription and s.payment_state" in self.queries[-1] else []

    def fetchone(self):
        return (180,) if "count(*)" in self.queries[-1] else ("SM-000001",)


def test_only_subscriptions_with_a_paid_first_order_are_loaded_with_their_renewal_count():
    cursor = _StateCursor()
    loaded = simulator.load_state(cursor, DAY).subscriptions
    assert loaded == [Subscription(500, 50, 2, datetime(2026, 9, 10, 9), renewals=3)]
    assert any("s.payment_state in ('paid', 'partially_refunded', 'refunded')" in sql for sql in cursor.queries)


def test_the_sign_up_rate_averages_three_months_so_a_slow_start_does_not_sink_a_brand():
    assert simulator.load_state(_StateCursor(), DAY).new_customers_per_day == pytest.approx(180 / 90 * 1.01)


def test_rows_stay_in_the_days_block_and_add_up():
    day = plan(state([], refund_candidates=[(i, f"SM-{i:06d}", 5000) for i in range(1, 200)]))
    block = simulator.id_block(DAY)
    for table in simulator.TABLES:
        assert all(row["id"] in block for row in getattr(day, table)), table
    order_ids = {o["id"] for o in day.orders}
    assert all(item["order_id"] in order_ids for item in day.order_items)
    for order in day.orders:
        lines = sum(i["total"] for i in day.order_items if i["order_id"] == order["id"])
        assert order["items_total"] == lines
        assert order["total"] == lines + order["adjustments_total"]
        assert order["number"] == f"SM-{order['id']:06d}"
        assert order["updated_at"] == NOW              # what the incremental load picks up
    completed = [r for r in day.refund_payments if r["state"] == "completed"]
    assert len(day.refunded_orders) == len(completed) > 0


def test_a_day_always_comes_out_the_same():
    subs = [Subscription(500 + i, 50 + i, 2, datetime(2026, 9, 10 + i % 15, 9), 0) for i in range(30)]
    assert plan(state(subs)).orders == plan(state(subs)).orders
    assert plan(state(subs)).orders != plan(state(subs), seed="wecare:2026-10-21").orders


def test_every_campaign_spends_about_its_recent_daily_budget():
    day = plan(state([]))
    assert [(r["utm_source"], r["utm_campaign"]) for r in day.marketing_spend] == \
        [(b.utm_source, b.utm_campaign) for b in BUDGETS]
    assert all(r["spend_date"] == DAY and r["updated_at"] == NOW for r in day.marketing_spend)
    assert 250 < sum(r["amount"] for r in day.marketing_spend) / len(BUDGETS) < 1000
    assert day.counts["ad_spend_cents"] == sum(r["amount"] for r in day.marketing_spend)
    assert plan(state([], budgets=[])).marketing_spend == []    # a brand with no ads spends nothing


class _PreviousDayCursor:
    def __init__(self):
        self.answers = [(0,), (0,)]     # this day's orders, the previous day's marketing spend

    def execute(self, sql, params=None):
        pass

    def fetchone(self):
        return self.answers.pop(0)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _PreviousDayConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_a_day_is_refused_when_the_previous_day_is_missing(monkeypatch):
    cursor = _PreviousDayCursor()
    monkeypatch.setattr(
        simulator.source_postgres, "connect",
        lambda database, conn_id=None: _PreviousDayConnection(cursor),
    )
    with pytest.raises(RuntimeError, match="is not in the CRM"):
        simulator.simulate_day("broomx", "crm_broomx", simulator.FIRST_DAY + timedelta(days=1))
