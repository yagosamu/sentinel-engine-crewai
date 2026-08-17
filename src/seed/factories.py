"""Frozen domain records and the factory that builds a correlated baseline.

The factory exists to produce a *clean*, referentially-plausible dataset so
that defects later injected by the chaos generator are unambiguous. The
invariants it upholds:

* money is always :class:`~decimal.Decimal` (2 dp, ``ROUND_HALF_UP``) via
  :func:`_money` -- never float in the database;
* ``cost`` is 45-80% of ``unit_price``, both inside the product category's
  price band (see ``CATEGORIES``);
* timestamps are causally ordered -- ``customer.created_at`` <=
  ``order.ordered_at`` <= ``payment.paid_at`` -- because each event is sampled
  after the prior one;
* a ``returned`` order forces ``payment.status = 'refunded'``;
* ``email`` and ``sku`` are unique (``faker.unique``).

The records are ``frozen``/``slots`` dataclasses so a built baseline cannot be
mutated in place.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal

from faker import Faker

CATEGORIES = {
    "Electronics": (49.0, 1999.0),
    "Home & Kitchen": (9.0, 499.0),
    "Apparel": (12.0, 249.0),
    "Beauty": (5.0, 149.0),
    "Sports": (15.0, 899.0),
    "Books": (4.0, 79.0),
    "Toys": (6.0, 199.0),
    "Grocery": (1.0, 89.0),
}

SEGMENTS = ("consumer", "prime", "business", "wholesale")
ORDER_STATUSES = ("placed", "shipped", "delivered", "returned", "cancelled")
PAYMENT_METHODS = ("credit_card", "debit_card", "paypal", "bank_transfer", "wallet")
PAYMENT_STATUSES = ("authorized", "captured", "refunded", "failed")


def _money(value: float) -> Decimal:
    """Quantise a float to a 2-dp ``Decimal`` (``ROUND_HALF_UP``).

    Converts via ``str(value)`` first to avoid binary-float artifacts, so money
    is exact before it reaches the database.
    """
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@dataclass(slots=True, frozen=True)
class Customer:
    full_name: str
    email: str
    country: str
    city: str
    segment: str
    created_at: datetime


@dataclass(slots=True, frozen=True)
class Product:
    sku: str
    name: str
    category: str
    unit_price: Decimal
    cost: Decimal
    created_at: datetime


@dataclass(slots=True, frozen=True)
class Order:
    customer_id: int
    product_id: int
    quantity: int
    unit_price: Decimal
    total_amount: Decimal
    status: str
    ordered_at: datetime


@dataclass(slots=True, frozen=True)
class Payment:
    order_id: int
    method: str
    amount: Decimal
    status: str
    paid_at: datetime


class EcommerceFactory:
    """Builds correlated ``Customer``/``Product``/``Order``/``Payment`` records.

    Determinism is the caller's responsibility: seed the shared ``Faker`` (via
    ``Faker.seed``) before building so the same seed yields the same rows.
    """

    def __init__(self, faker: Faker) -> None:
        self.faker = faker

    def customer(self) -> Customer:
        """Build one customer with a unique email and a 1-3y-old signup date."""
        name = self.faker.name()
        handle = name.lower().replace(" ", ".").replace("'", "")
        return Customer(
            full_name=name,
            email=f"{handle}.{self.faker.unique.random_int(1000, 999999)}@{self.faker.free_email_domain()}",
            country=self.faker.country(),
            city=self.faker.city(),
            segment=self.faker.random_element(SEGMENTS),
            created_at=self.faker.date_time_between(start_date="-3y", end_date="-1d", tzinfo=UTC),
        )

    def product(self) -> Product:
        """Build one product priced inside its category band, cost 45-80% of it."""
        category = self.faker.random_element(list(CATEGORIES))
        low, high = CATEGORIES[category]
        unit_price = _money(self.faker.pyfloat(min_value=low, max_value=high, right_digits=2))
        cost = _money(float(unit_price) * self.faker.pyfloat(min_value=0.45, max_value=0.8, right_digits=2))
        noun = category[:-1] if category.endswith("s") else category
        name = f"{self.faker.color_name()} {self.faker.word().title()} {noun}"
        return Product(
            sku=self.faker.unique.bothify(text="???-########").upper(),
            name=name,
            category=category,
            unit_price=unit_price,
            cost=cost,
            created_at=self.faker.date_time_between(start_date="-3y", end_date="-1d", tzinfo=UTC),
        )

    def order(self, customer_id: int, product: tuple[int, Decimal], not_before: datetime) -> Order:
        """Build one order; ``ordered_at`` is sampled at or after ``not_before``.

        Pass the customer's ``created_at`` as ``not_before`` to keep the
        signup <= order temporal invariant. ``total_amount`` is
        ``unit_price * quantity`` rounded as money.
        """
        product_id, unit_price = product
        quantity = self.faker.random_int(1, 6)
        total = _money(float(unit_price) * quantity)
        ordered_at = self.faker.date_time_between(start_date=not_before, end_date="now", tzinfo=UTC)
        return Order(
            customer_id=customer_id,
            product_id=product_id,
            quantity=quantity,
            unit_price=unit_price,
            total_amount=total,
            status=self.faker.random_element(ORDER_STATUSES),
            ordered_at=ordered_at,
        )

    def payment(self, order_id: int, order: Order) -> Payment:
        """Build the matching payment; a returned order forces ``refunded``.

        ``paid_at`` is sampled at or after ``order.ordered_at`` and the amount
        mirrors the order total, preserving the order <= payment invariant.
        """
        status = "refunded" if order.status == "returned" else self.faker.random_element(PAYMENT_STATUSES)
        paid_at = self.faker.date_time_between(start_date=order.ordered_at, end_date="now", tzinfo=UTC)
        return Payment(
            order_id=order_id,
            method=self.faker.random_element(PAYMENT_METHODS),
            amount=order.total_amount,
            status=status,
            paid_at=paid_at,
        )
