#!/usr/bin/env python3
"""
generate_events.py — Generate synthetic Kafka event payloads for a single event type.

Usage:
    python3 scripts/generate_events.py <EVENT_TYPE> <count> <cache_dir> [--append]

Arguments:
    EVENT_TYPE   One of the 15 supported event types (e.g. ORDER_CREATED)
    count        Number of events to generate
    cache_dir    Directory where <EVENT_TYPE>.json cache files live
    --append     Append generated events to the existing cache file instead of
                 overwriting it.  Use this when the cache has fewer events than
                 the requested sample size and you want to top it up.

Exit codes:
    0  success
    1  unknown event type or write error

Examples:
    # Create (or overwrite) a cache file with 500 ORDER_CREATED events
    python3 scripts/generate_events.py ORDER_CREATED 500 ./events-cache

    # Append 200 more PAYMENT_FAILED events to an existing cache file
    python3 scripts/generate_events.py PAYMENT_FAILED 200 ./events-cache --append
"""

import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rand_uuid() -> str:
    return str(uuid.uuid4())


def rand_ts() -> str:
    """Return a random ISO-8601 timestamp between 2024-01-01 and 2026-12-31."""
    base = datetime(2024, 1, 1)
    delta = timedelta(
        days=random.randint(0, 1094),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return (base + delta).strftime("%Y-%m-%dT%H:%M:%SZ")


def cycle(seq, i: int):
    """Return seq[i % len(seq)] — used to spread enum values evenly."""
    return seq[i % len(seq)]


# ---------------------------------------------------------------------------
# Per-event-type generators
# ---------------------------------------------------------------------------

_CURRENCIES = ["USD", "EUR", "GBP"]
_CARRIERS = ["FEDEX", "UPS", "DHL", "USPS"]
_CANCEL_REASONS = ["CUSTOMER_REQUEST", "OUT_OF_STOCK", "PAYMENT_FAILED", "FRAUD_DETECTED"]
_PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"]
_ERROR_CODES = ["INSUFFICIENT_FUNDS", "CARD_DECLINED", "TIMEOUT", "FRAUD_BLOCKED"]
_ERROR_MESSAGES = {
    "INSUFFICIENT_FUNDS": [
        "Insufficient funds in account",
        "Account balance too low to complete transaction",
        "Payment declined: insufficient account balance",
    ],
    "CARD_DECLINED": [
        "Card was declined by the issuer",
        "Card issuer rejected the charge",
        "Payment card declined — contact your bank",
    ],
    "TIMEOUT": [
        "Payment gateway timeout — please retry",
        "Gateway did not respond in time",
        "Request timed out, please try again",
        "Connection to payment provider timed out",
    ],
    "FRAUD_BLOCKED": [
        "Transaction blocked by fraud detection",
        "Suspicious activity detected — payment blocked",
        "Fraud risk threshold exceeded",
    ],
}
_COUNTRIES = ["US", "GB", "DE", "FR", "JP", "CA", "AU", "BR", "IN", "MX"]
_DELETE_REASONS = ["USER_REQUEST", "GDPR_ERASURE", "ADMIN_ACTION"]
_CHANGED_FIELD_COMBOS = [
    ["email"],
    ["name"],
    ["country"],
    ["email", "name"],
    ["name", "country"],
    ["email", "country"],
    ["email", "name", "country"],
]
_PRODUCT_IDS = [f"PROD-{i:04d}" for i in range(1, 51)]
_FIRST_NAMES = ["Alice", "Bob", "Carlos", "Diana", "Ethan", "Fatima", "George", "Hannah", "Ivan", "Julia"]
_LAST_NAMES = ["Smith", "Johnson", "Garcia", "Brown", "Lee", "Kim", "Patel", "Singh", "Wang", "Chen"]
_EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "example.com"]
_ITEM_NAMES = ["Widget", "Gadget", "Doohickey", "Thingamajig", "Gizmo",
               "Sprocket", "Cog", "Bolt", "Lever", "Pulley",
               "Wrench", "Gear", "Rivet", "Clamp", "Hinge"]
_ITEM_CATEGORIES = ["electronics", "clothing", "food", "sports"]
_REMOVE_REASONS = ["out-of-stock", "discontinued", "damaged", "recalled"]
_ADJUST_EMAILS = [
    "admin@example.com", "ops@warehouse.com", "manager@supply.com",
    "stock@fulfillment.com", "supervisor@warehouse.com", "lead@ops.com",
    "coordinator@supply.com", "analyst@logistics.com", "planner@inventory.com",
    "director@supply.com", "controller@warehouse.com", "auditor@ops.com",
]
_SERVICE_IDS = [f"svc-{i:03d}" for i in range(1, 21)]
_BANK_NAMES = ["First National", "City Trust", "Coastal Bank",
               "Union Federal", "Heritage Savings"]

# Edge-case data pools
_CC_SOURCES = ["web", "mobile", "api"]
_FP_STATUSES = {"FP_PAYMENT_INITIATED": "NEW", "FP_PAYMENT_COMPLETED": "DONE"}
_EC5_COUNTRIES = ["US", "IN"]   # 2-value faithful to client literal; gap = 10 (B2)
_EC4_UPDATE_REASONS = ["deprecated", "migrated", "corrected", "revised", "patched"]
_EC4_DELETE_REASONS = ["removed", "archived", "purged", "expired", "revoked"]


def _gen_order_created(i: int) -> dict:
    n_items = random.randint(1, 5)
    items = [
        {
            "productId": random.choice(_PRODUCT_IDS),
            "quantity": random.randint(1, 10),
            "unitPrice": round(random.uniform(5.0, 500.0), 2),
        }
        for _ in range(n_items)
    ]
    total = round(sum(it["quantity"] * it["unitPrice"] for it in items), 2)
    return {
        "eventType": "ORDER_CREATED",
        "orderId": rand_uuid(),
        "customerId": rand_uuid(),
        "items": items,
        "totalAmount": total,
        "currency": cycle(_CURRENCIES, i),
        "createdAt": rand_ts(),
    }


def _gen_order_shipped(i: int) -> dict:
    shipped = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 1000))
    estimated = shipped + timedelta(days=random.randint(1, 14))
    return {
        "eventType": "ORDER_SHIPPED",
        "orderId": rand_uuid(),
        "trackingNumber": f"TRK{random.randint(100_000_000, 999_999_999)}",
        "carrier": cycle(_CARRIERS, i),
        "shippedAt": shipped.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "estimatedDelivery": estimated.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _gen_order_cancelled(i: int) -> dict:
    return {
        "eventType": "ORDER_CANCELLED",
        "orderId": rand_uuid(),
        "reason": cycle(_CANCEL_REASONS, i),
        "cancelledAt": rand_ts(),
        "refundAmount": round(random.uniform(0.0, 2000.0), 2),
    }


def _gen_payment_initiated(i: int) -> dict:
    return {
        "eventType": "PAYMENT_INITIATED",
        "paymentId": rand_uuid(),
        "orderId": rand_uuid(),
        "amount": round(random.uniform(5.0, 2000.0), 2),
        "currency": cycle(_CURRENCIES, i),
        "method": cycle(_PAYMENT_METHODS, i),
        "initiatedAt": rand_ts(),
    }


def _gen_payment_completed(_i: int) -> dict:
    return {
        "eventType": "PAYMENT_COMPLETED",
        "paymentId": rand_uuid(),
        "transactionId": f"TXN-{random.randint(10_000_000, 99_999_999)}",
        "amount": round(random.uniform(5.0, 2000.0), 2),
        "completedAt": rand_ts(),
    }


def _gen_payment_failed(i: int) -> dict:
    code = cycle(_ERROR_CODES, i)
    return {
        "eventType": "PAYMENT_FAILED",
        "paymentId": rand_uuid(),
        "errorCode": code,
        "errorMessage": random.choice(_ERROR_MESSAGES[code]),
        "failedAt": rand_ts(),
    }


def _gen_user_registered(i: int) -> dict:
    name = f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"
    email = f"{name.lower().replace(' ', '.')}{random.randint(1, 999)}@{random.choice(_EMAIL_DOMAINS)}"
    return {
        "eventType": "USER_REGISTERED",
        "userId": rand_uuid(),
        "email": email,
        "name": name,
        "country": cycle(_COUNTRIES, i),
        "registeredAt": rand_ts(),
    }


def _gen_user_updated(i: int) -> dict:
    return {
        "eventType": "USER_UPDATED",
        "userId": rand_uuid(),
        "changedFields": cycle(_CHANGED_FIELD_COMBOS, i),
        "updatedAt": rand_ts(),
    }


def _gen_user_deleted(i: int) -> dict:
    return {
        "eventType": "USER_DELETED",
        "userId": rand_uuid(),
        "reason": cycle(_DELETE_REASONS, i),
        "deletedAt": rand_ts(),
    }


def _gen_item_added(i: int) -> dict:
    return {
        "action":   "ITEM_ADDED",
        "itemId":   rand_uuid(),
        "name":     random.choice(_ITEM_NAMES),
        "quantity": random.randint(1, 100),
        "category": cycle(_ITEM_CATEGORIES, i),
    }


def _gen_item_removed(i: int) -> dict:
    return {
        "action": "ITEM_REMOVED",
        "itemId": rand_uuid(),
        "reason": cycle(_REMOVE_REASONS, i),
    }


def _gen_inventory_adjusted(i: int) -> dict:
    delta = random.choice([d for d in range(-50, 51) if d != 0])
    return {
        "action":     "INVENTORY_ADJUSTED",
        "itemId":     rand_uuid(),
        "delta":      delta,
        "adjustedBy": cycle(_ADJUST_EMAILS, i),
    }


def _gen_heartbeat(i: int) -> dict:
    # No action/eventType field — exactly 4 fields always, same structure every time.
    # This is intentional: kafka-spy must infer NoVariants → SingleType.
    return {
        "serviceId":     cycle(_SERVICE_IDS, i),
        "timestamp":     rand_ts(),
        "healthy":       random.choice([True, False]),
        "uptimeSeconds": random.randint(0, 86400),
    }


def _gen_card_payment(_i: int) -> dict:
    # No eventType, action, type, kind — only field shared with BANK_TRANSFER is 'amount'.
    month = random.randint(1, 12)
    year = random.randint(25, 30)
    return {
        "cardNumber": "".join(str(random.randint(0, 9)) for _ in range(16)),
        "expiry":     f"{month:02d}/{year}",
        "cvv":        "".join(str(random.randint(0, 9)) for _ in range(3)),
        "amount":     round(random.uniform(1.0, 999.99), 2),
    }


def _gen_bank_transfer(_i: int) -> dict:
    # No eventType, action, type, kind — shares only 'amount' with CARD_PAYMENT.
    return {
        "routingNumber": "".join(str(random.randint(0, 9)) for _ in range(9)),
        "accountNumber": "".join(str(random.randint(0, 9)) for _ in range(random.randint(8, 12))),
        "bankName":      random.choice(_BANK_NAMES),
        "amount":        round(random.uniform(1.0, 9999.99), 2),
    }


# ---------------------------------------------------------------------------
# EC1 — competing-candidates-events
# eventType ∈ {ORDER_CREATED, ORDER_CANCELLED}, source cross-cuts via offset so
# the two fields never accidentally correlate.  orderId is a uuid (unique-per-
# record) so it collects the all-unique -30 penalty and cannot enter candidacy.
# ---------------------------------------------------------------------------

def _gen_cc_order_created(i: int) -> dict:
    return {
        "eventType": "ORDER_CREATED",
        "source":    cycle(_CC_SOURCES, i),           # offset-0: web,mobile,api,...
        "orderId":   rand_uuid(),                      # unique per record
        "totalAmount": round(random.uniform(10.0, 500.0), 2),
    }


def _gen_cc_order_cancelled(i: int) -> dict:
    return {
        "eventType": "ORDER_CANCELLED",
        "source":    cycle(_CC_SOURCES, i + 1),       # offset-1: mobile,api,web,...
        "orderId":   rand_uuid(),                      # unique per record
        "refundAmount": round(random.uniform(0.0, 500.0), 2),
    }


# ---------------------------------------------------------------------------
# EC2 — false-positive-events
# status is perfectly correlated 1:1 with eventType (NEW↔INITIATED,
# DONE↔COMPLETED).  Same field-set → single cluster → purity identical for
# both.  Only the semantic bias (+10 eventType vs -10 status) separates them.
# paymentId is uuid → all-unique penalty, out of contention.
# ---------------------------------------------------------------------------

def _gen_fp_payment_initiated(i: int) -> dict:
    return {
        "eventType": "PAYMENT_INITIATED",
        "status":    "NEW",
        "paymentId": rand_uuid(),                      # unique per record
        "amount":    round(random.uniform(5.0, 2000.0), 2),
    }


def _gen_fp_payment_completed(i: int) -> dict:
    return {
        "eventType": "PAYMENT_COMPLETED",
        "status":    "DONE",
        "paymentId": rand_uuid(),                      # unique per record
        "amount":    round(random.uniform(5.0, 2000.0), 2),
    }


# ---------------------------------------------------------------------------
# EC3-strict — partial-field-events
# 60 % of payloads include eventType; 40 % do not.  Two clusters result, each
# sharing the {userId, email} core → cluster-2 has no unique signature path →
# deriveImplicitShape returns null → None → SINGLE_TYPE.
# eventType is optional in the merged schema, required fields are userId+email.
# ---------------------------------------------------------------------------

def _gen_partial_field(i: int) -> dict:
    first = random.choice(_FIRST_NAMES)
    last = random.choice(_LAST_NAMES)
    email = f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{random.choice(_EMAIL_DOMAINS)}"
    base = {"userId": rand_uuid(), "email": email}
    if i % 5 < 3:                                      # 60 % include eventType
        base["eventType"] = "USER_REGISTERED"
    return base


# ---------------------------------------------------------------------------
# EC3-literal — partial-field-literal-events  (negotiate-class, infra only)
# Both payload shapes differ beyond the missing field: one has eventType, the
# other has name.  Two clusters each have a non-empty unique signature →
# IMPLICIT_SHAPE.  No EXPECTED entry until D1 is resolved.
# ---------------------------------------------------------------------------

def _gen_partial_literal(i: int) -> dict:
    first = random.choice(_FIRST_NAMES)
    last = random.choice(_LAST_NAMES)
    email = f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{random.choice(_EMAIL_DOMAINS)}"
    if i % 2 == 0:
        return {"eventType": "USER_REGISTERED", "userId": rand_uuid(), "email": email}
    else:
        return {"userId": rand_uuid(), "email": email, "name": f"{first} {last}"}


# ---------------------------------------------------------------------------
# EC4 — overlapping-values-events  (negotiate-class, infra only)
# type="UPDATE" maps to two distinct shapes (name vs amount); type="DELETE" is
# a third shape (reason).  Shapes have disjoint unique fields.
# Self-check: _EC4_SHAPE_FIELDS must be pairwise disjoint.
# ---------------------------------------------------------------------------

_EC4_SHAPE_FIELDS = {
    "A": {"name"},
    "B": {"amount"},
    "C": {"reason"},
}
assert all(
    _EC4_SHAPE_FIELDS[a].isdisjoint(_EC4_SHAPE_FIELDS[b])
    for a in _EC4_SHAPE_FIELDS for b in _EC4_SHAPE_FIELDS if a != b
), "EC4 shape fields must be disjoint"


def _gen_overlapping_value(i: int) -> dict:
    ts = rand_ts()
    bucket = i % 5
    if bucket <= 1:                                    # 40 % UPDATE+name
        return {"type": "UPDATE", "name": random.choice(_FIRST_NAMES), "updatedAt": ts}
    elif bucket <= 3:                                  # 40 % UPDATE+amount
        return {"type": "UPDATE", "amount": round(random.uniform(1.0, 999.0), 2), "updatedAt": ts}
    else:                                              # 20 % DELETE+reason
        return {"type": "DELETE", "reason": random.choice(_EC4_DELETE_REASONS), "updatedAt": ts}


# ---------------------------------------------------------------------------
# EC5 — small-sample-events
# country cycles through exactly 2 values (faithful to client's literal US/IN
# data; B2 documents that the gap = 10 and D4-a must use X < 10).
# At total=2 (sample_size=1 per type) both eventType and country are all-unique
# → SINGLE_TYPE.  At total≥4 (sample_size≥2) eventType wins via +10 semantic.
# userId is uuid (unique per record, all-unique penalty, out of contention).
# ---------------------------------------------------------------------------

def _gen_sm_user_registered(i: int) -> dict:
    return {
        "eventType": "USER_REGISTERED",
        "country":   cycle(_EC5_COUNTRIES, i),
        "userId":    rand_uuid(),
    }


def _gen_sm_user_deleted(i: int) -> dict:
    return {
        "eventType": "USER_DELETED",
        "country":   cycle(_EC5_COUNTRIES, i + 1),    # offset-1 vs registered
        "userId":    rand_uuid(),
    }


# ---------------------------------------------------------------------------
# EC7 — generic-events  (already covered, optional confirmation topic)
# All payloads share one structure with no enum-like field → NoVariants →
# SINGLE_TYPE.  id is uuid (no bias, all-unique), amount numeric, timestamp str.
# ---------------------------------------------------------------------------

def _gen_generic_event(_i: int) -> dict:
    return {
        "id":        rand_uuid(),
        "amount":    round(random.uniform(1.0, 1000.0), 2),
        "timestamp": rand_ts(),
    }


_GENERATORS = {
    "ORDER_CREATED":      _gen_order_created,
    "ORDER_SHIPPED":      _gen_order_shipped,
    "ORDER_CANCELLED":    _gen_order_cancelled,
    "PAYMENT_INITIATED":  _gen_payment_initiated,
    "PAYMENT_COMPLETED":  _gen_payment_completed,
    "PAYMENT_FAILED":     _gen_payment_failed,
    "USER_REGISTERED":    _gen_user_registered,
    "USER_UPDATED":       _gen_user_updated,
    "USER_DELETED":       _gen_user_deleted,
    "ITEM_ADDED":         _gen_item_added,
    "ITEM_REMOVED":       _gen_item_removed,
    "INVENTORY_ADJUSTED": _gen_inventory_adjusted,
    "HEARTBEAT":          _gen_heartbeat,
    "CARD_PAYMENT":       _gen_card_payment,
    "BANK_TRANSFER":      _gen_bank_transfer,
    # Edge-case types
    "CC_ORDER_CREATED":      _gen_cc_order_created,
    "CC_ORDER_CANCELLED":    _gen_cc_order_cancelled,
    "FP_PAYMENT_INITIATED":  _gen_fp_payment_initiated,
    "FP_PAYMENT_COMPLETED":  _gen_fp_payment_completed,
    "PARTIAL_FIELD_EVENT":   _gen_partial_field,
    "PARTIAL_LITERAL_EVENT": _gen_partial_literal,
    "OVERLAPPING_VALUE_EVENT": _gen_overlapping_value,
    "SM_USER_REGISTERED":    _gen_sm_user_registered,
    "SM_USER_DELETED":       _gen_sm_user_deleted,
    "GENERIC_EVENT":         _gen_generic_event,
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = sys.argv[1:]

    # Parse --append flag
    append = "--append" in args
    args = [a for a in args if a != "--append"]

    if len(args) < 3:
        print(__doc__)
        sys.exit(1)

    event_type = args[0].upper()
    try:
        count = int(args[1])
    except ValueError:
        print(f"ERROR: count must be an integer, got '{args[1]}'", file=sys.stderr)
        sys.exit(1)
    cache_dir = args[2]

    if event_type not in _GENERATORS:
        print(
            f"ERROR: unknown event type '{event_type}'. "
            f"Valid types: {', '.join(sorted(_GENERATORS))}",
            file=sys.stderr,
        )
        sys.exit(1)

    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, f"{event_type}.json")

    # Load existing events if appending
    existing: list = []
    if append and os.path.exists(cache_file):
        with open(cache_file) as f:
            existing = json.load(f)
        start_index = len(existing)
    else:
        start_index = 0

    generator = _GENERATORS[event_type]
    new_events = [generator(start_index + i) for i in range(count)]
    all_events = existing + new_events

    with open(cache_file, "w") as f:
        json.dump(all_events, f, indent=2)

    action = "appended" if append and existing else "generated"
    print(
        f"{'📝'} {event_type}: {action} {count} events "
        f"(cache total: {len(all_events)})"
    )


if __name__ == "__main__":
    main()
