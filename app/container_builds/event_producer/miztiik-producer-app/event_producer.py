import json
import logging
import datetime
import time
import os
import random
import uuid
import socket

# ANSI color codes
GREEN_COLOR = "\033[32m"
RED_COLOR = "\033[31m"
RESET_COLOR = "\033[0m"


class GlobalArgs:
    OWNER = "Mystique"
    VERSION = "2023-06-26"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    EVNT_WEIGHTS = {"success": 80, "fail": 20}
    TRIGGER_RANDOM_FAILURES = os.getenv("TRIGGER_RANDOM_FAILURES", True)
    WAIT_SECS_BETWEEN_MSGS = int(os.getenv("WAIT_SECS_BETWEEN_MSGS", 2))
    TOT_MSGS_TO_PRODUCE = int(os.getenv("TOT_MSGS_TO_PRODUCE", 10))


def _rand_coin_flip():
    r = False
    if GlobalArgs.TRIGGER_RANDOM_FAILURES:
        r = random.choices([True, False], weights=[0.1, 0.9], k=1)[0]
    return r


def _gen_uuid():
    return str(uuid.uuid4())


def generate_event():

    # Following Patterns are implemented
    # If event_type is inventory_event, then is_return is True for 50% of the events
    # 10% of total events are poison pill events, bad_msg attribute is True and store_id is removed
    # Event attributes are set with priority_shipping, is_return, and event type

    _categories = ["Books", "Games", "Mobiles", "Groceries", "Shoes", "Stationaries", "Laptops", "Tablets",
                   "Notebooks", "Camera", "Printers", "Monitors", "Speakers", "Projectors", "Cables", "Furniture"]
    _variants = ["black", "red"]
    _evnt_types = ["sale_event", "inventory_event"]
    _currencies = ["USD", "INR", "EUR", "GBP",
                   "AUD", "CAD", "SGD", "JPY", "CNY", "HKD"]
    _payments = ["credit_card", "debit_card", "cash",
                 "wallet", "upi", "net_banking", "cod", "gift_card"]

    _qty = random.randint(1, 99)
    _s = round(random.random() * 100, 2)

    _evnt_type = random.choices(_evnt_types, weights=[0.8, 0.2], k=1)[0]
    _u = _gen_uuid()
    p_s = random.choices([True, False], weights=[0.3, 0.7], k=1)[0]
    is_return = False

    if _evnt_type == "inventory_event":
        is_return = bool(random.getrandbits(1))

    evnt_body = {
        "id": _u,
        "event_type": _evnt_type,
        "store_id": random.randint(1, 10),
        "store_fqdn": str(socket.getfqdn()),
        "store_ip": str(socket.gethostbyname(socket.gethostname())),
        "cust_id": random.randint(100, 999),
        "category": random.choice(_categories),
        "sku": random.randint(18981, 189281),
        "price": _s,
        "qty": _qty,
        "currency": random.choice(_currencies),
        "discount": random.randint(0, 75),
        "gift_wrap": random.choices([True, False], weights=[0.3, 0.7], k=1)[0],
        "variant": random.choice(_variants),
        "priority_shipping": p_s,
        "payment_method": random.choice(_payments),
        "ts": datetime.datetime.now().isoformat(),
        "contact_me": "github.com/miztiik",
        "is_return": is_return
    }

    if _rand_coin_flip():
        evnt_body.pop("store_id", None)
        evnt_body["bad_msg"] = True

    _attr = {
        "event_type": _evnt_type,
        "priority_shipping": str(p_s),
        "is_return": str(is_return)
    }

    return evnt_body, _attr


def evnt_producer():
    resp = {
        "status": False,
        "tot_msgs": 0,
        "event_sample": None
    }

    try:
        t_msgs = 0
        p_cnt = 0
        s_evnts = 0
        inventory_evnts = 0
        t_sales = 0

        # Start timing the event generation
        event_gen_start_time = time.time()

        while t_msgs < GlobalArgs.TOT_MSGS_TO_PRODUCE:
            evnt_body, evnt_attr = generate_event()
            t_msgs += 1
            t_sales += evnt_body["price"] * evnt_body["qty"]

            if evnt_body.get("bad_msg"):
                p_cnt += 1

            if evnt_attr["event_type"] == "sale_event":
                s_evnts += 1
            elif evnt_attr["event_type"] == "inventory_event":
                inventory_evnts += 1

            if t_msgs == 1:
                resp["event_sample"] = evnt_body

            time.sleep(GlobalArgs.WAIT_SECS_BETWEEN_MSGS)
            logging.info(f"generated_event:{json.dumps(evnt_body)}")

        event_gen_end_time = time.time()  # Stop timing the event generation
        event_gen_duration = event_gen_end_time - \
            event_gen_start_time  # Calculate the duration

        resp["event_gen_duration"] = event_gen_duration
        resp["tot_msgs"] = t_msgs
        resp["bad_msgs"] = p_cnt
        resp["sale_evnts"] = s_evnts
        resp["inventory_evnts"] = inventory_evnts
        resp["tot_sales"] = t_sales
        resp["status"] = True

    except Exception as e:
        logging.error(f"ERROR: {type(e).__name__}: {str(e)}")
        resp["err_msg"] = f"ERROR: {type(e).__name__}: {str(e)}"

    return resp


if __name__ == "__main__":
    evnt_producer()
