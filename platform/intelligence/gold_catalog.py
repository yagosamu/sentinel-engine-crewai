"""The gold-layer catalog: the single source of truth for what C5 exposes.

C5 is a *read model* over the ``gold`` schema ONLY. Everything the three MCP
tools are allowed to touch is declared here:

* :data:`GOLD_TABLES` — the table allowlist. It is enforced BY CONSTRUCTION:
  ``execute_analytical_query`` never accepts raw SQL — it only runs the curated
  intent templates below, every one of which references ONLY relations in this
  set. ``get_schema_info`` reflects exactly these tables. (The SQL guard in
  ``sql_guard.py`` is a SEPARATE, write-blocking layer; it does NOT parse or
  restrict relation names — the allowlist is upheld by the templates, not the
  guard. Keep that distinction true if a raw-SQL intent is ever added.)
* Named **intents** — the curated, parameterized analytical questions
  ``execute_analytical_query`` understands. An intent maps NL-shaped input to a
  *fixed* SQL template with bound parameters; the LLM never hands us raw SQL.

Why a static allowlist and not "ask DuckDB for every gold table"? Because the
allowlist is the exposure boundary the curated intents are authored against. If a
future model lands in ``gold`` that we do not want exposed (PII rollups, internal
audit tables), it stays invisible to the LLM as long as no intent references it —
because the intents are the only SQL that ever runs. Allowlist > denylist.

This module imports nothing from Postgres and never opens a writable handle.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# --- Table allowlist -------------------------------------------------------

GOLD_SCHEMA = "gold"

# The exact gold relations C5 will expose. Keep in lock-step with the dbt gold
# models (platform/transform/models/gold/*.sql). Adding a table here is a
# deliberate act of exposure.
GOLD_TABLES: tuple[str, ...] = (
    "gold_orders_obt",
    "gold_revenue_daily",
)


def qualified(table: str) -> str:
    """Return the schema-qualified name (``gold.<table>``) for a known table."""
    if table not in GOLD_TABLES:
        raise KeyError(f"{table!r} is not an exposed gold table")
    return f"{GOLD_SCHEMA}.{table}"


# --- Curated analytical intents -------------------------------------------


@dataclass(frozen=True, slots=True)
class IntentParam:
    """One parameter accepted by an intent, with its JSON-schema fragment."""

    name: str
    json_type: str
    description: str
    required: bool = False
    default: object | None = None
    enum: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class Intent:
    """A curated analytical question: fixed SQL + bound parameter contract.

    ``sql`` uses DuckDB ``$name`` placeholders. The engine binds *only* the
    declared params; it never string-formats user input into the SQL body. The
    one exception is :attr:`order_by_whitelist`, which the engine validates
    against before interpolating an identifier (never raw user text).
    """

    name: str
    title: str
    description: str
    sql: str
    params: tuple[IntentParam, ...] = ()
    # Columns a caller may sort by (identifier interpolation is validated
    # against this set only — never against arbitrary input).
    order_by_whitelist: tuple[str, ...] = ()
    default_order_by: str | None = None


# Each SQL body references ONLY allowlisted gold tables and binds every
# user-supplied value as a $param. ``limit`` is universal and clamped by the
# engine, so it is not redeclared per-intent.

INTENTS: dict[str, Intent] = {
    "revenue_by_period": Intent(
        name="revenue_by_period",
        title="Revenue by day",
        description=(
            "Total gross revenue, order count and units sold per day, "
            "optionally bounded to a [start_date, end_date] window and/or a "
            "single product category. Backed by gold_revenue_daily."
        ),
        sql="""
            select
                order_date,
                sum(order_count)   as order_count,
                sum(units_sold)    as units_sold,
                sum(gross_revenue) as gross_revenue
            from gold.gold_revenue_daily
            where ($start_date is null or order_date >= $start_date)
              and ($end_date   is null or order_date <= $end_date)
              and ($category   is null or product_category = $category)
            group by order_date
        """,
        params=(
            IntentParam("start_date", "string", "Inclusive ISO date lower bound (YYYY-MM-DD)."),
            IntentParam("end_date", "string", "Inclusive ISO date upper bound (YYYY-MM-DD)."),
            IntentParam("category", "string", "Restrict to one product category."),
        ),
        order_by_whitelist=("order_date", "gross_revenue", "order_count", "units_sold"),
        default_order_by="order_date desc",
    ),
    "revenue_by_category": Intent(
        name="revenue_by_category",
        title="Revenue by product category",
        description=(
            "Gross revenue, order count and units sold rolled up per product "
            "category across an optional date window. Backed by "
            "gold_revenue_daily."
        ),
        sql="""
            select
                product_category,
                sum(order_count)   as order_count,
                sum(units_sold)    as units_sold,
                sum(gross_revenue) as gross_revenue
            from gold.gold_revenue_daily
            where ($start_date is null or order_date >= $start_date)
              and ($end_date   is null or order_date <= $end_date)
            group by product_category
        """,
        params=(
            IntentParam("start_date", "string", "Inclusive ISO date lower bound (YYYY-MM-DD)."),
            IntentParam("end_date", "string", "Inclusive ISO date upper bound (YYYY-MM-DD)."),
        ),
        order_by_whitelist=("product_category", "gross_revenue", "order_count", "units_sold"),
        default_order_by="gross_revenue desc",
    ),
    "revenue_by_country": Intent(
        name="revenue_by_country",
        title="Revenue by customer country",
        description=(
            "Gross revenue and order count per customer country. Realized "
            "demand only (cancelled/returned excluded). Backed by "
            "gold_orders_obt."
        ),
        sql="""
            select
                customer_country,
                count(*) as order_count,
                sum(
                    case when status in ('cancelled', 'returned')
                         then 0 else total_amount end
                ) as gross_revenue
            from gold.gold_orders_obt
            where ($start_date is null or cast(ordered_at as date) >= $start_date)
              and ($end_date   is null or cast(ordered_at as date) <= $end_date)
            group by customer_country
        """,
        params=(
            IntentParam("start_date", "string", "Inclusive ISO date lower bound (YYYY-MM-DD)."),
            IntentParam("end_date", "string", "Inclusive ISO date upper bound (YYYY-MM-DD)."),
        ),
        order_by_whitelist=("customer_country", "gross_revenue", "order_count"),
        default_order_by="gross_revenue desc",
    ),
    "top_products": Intent(
        name="top_products",
        title="Top products by revenue",
        description=(
            "Best-selling products ranked by realized gross revenue, with "
            "units sold and order count. Optional category filter. Backed by "
            "gold_orders_obt."
        ),
        sql="""
            select
                product_sku,
                product_name,
                product_category,
                count(*)       as order_count,
                sum(quantity)  as units_sold,
                sum(
                    case when status in ('cancelled', 'returned')
                         then 0 else total_amount end
                ) as gross_revenue
            from gold.gold_orders_obt
            where ($category is null or product_category = $category)
            group by product_sku, product_name, product_category
        """,
        params=(IntentParam("category", "string", "Restrict to one product category."),),
        order_by_whitelist=("gross_revenue", "units_sold", "order_count", "product_name"),
        default_order_by="gross_revenue desc",
    ),
    "order_status_breakdown": Intent(
        name="order_status_breakdown",
        title="Order status breakdown",
        description=(
            "Order count and total amount grouped by order status "
            "(placed/paid/shipped/cancelled/returned/...). Backed by "
            "gold_orders_obt."
        ),
        sql="""
            select
                status,
                count(*)           as order_count,
                sum(total_amount)  as total_amount,
                sum(case when is_late then 1 else 0 end) as late_count
            from gold.gold_orders_obt
            group by status
        """,
        order_by_whitelist=("order_count", "total_amount", "status"),
        default_order_by="order_count desc",
    ),
}


# --- Report definitions ----------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReportSection:
    """One section of a generated report: a title bound to an intent run."""

    key: str
    title: str
    intent: str
    # Static params merged under any caller-supplied window; caller params win.
    params: dict[str, object] = field(default_factory=dict)
    limit: int = 10


@dataclass(frozen=True, slots=True)
class ReportDef:
    """A named multi-section report assembled from curated intents."""

    name: str
    title: str
    description: str
    sections: tuple[ReportSection, ...]


REPORTS: dict[str, ReportDef] = {
    "executive_summary": ReportDef(
        name="executive_summary",
        title="Executive Revenue Summary",
        description=(
            "Cross-cut of the business: revenue by category, by country, the "
            "top products, and the order-status mix. Each section is a curated "
            "intent run, so the whole report is deterministic."
        ),
        sections=(
            ReportSection("by_category", "Revenue by Category", "revenue_by_category", limit=20),
            ReportSection("by_country", "Revenue by Country", "revenue_by_country", limit=20),
            ReportSection("top_products", "Top 10 Products", "top_products", limit=10),
            ReportSection("status_mix", "Order Status Breakdown", "order_status_breakdown", limit=20),
        ),
    ),
    "daily_revenue": ReportDef(
        name="daily_revenue",
        title="Daily Revenue Report",
        description=(
            "Day-by-day revenue trend plus the category split for the same "
            "window. Pass start_date/end_date to bound it."
        ),
        sections=(
            ReportSection("trend", "Daily Revenue Trend", "revenue_by_period", limit=60),
            ReportSection("category_split", "Category Split", "revenue_by_category", limit=20),
        ),
    ),
}


# Universal limit guardrails applied to every intent run.
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
