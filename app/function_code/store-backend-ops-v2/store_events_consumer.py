import json
import logging
import datetime

import azure.functions as func
from az_utils import (
    write_to_blob,
    write_to_cosmosdb,
    write_to_svc_bus_q,
    write_to_svc_bus_topic,
    write_to_event_hub,
)


class GlobalArgs:
    OWNER = "Mystique"
    VERSION = "2024-04-08"


def evnt_consumer():
    # Following Patterns are implemented
    # If event_type is inventory_event, then is_return is True for 50% of the events
    # 10% of total events are poison pill events, bad_msg attribute is True and store_id is removed
    # Event attributes
    return True


def process_q_msg(msg: func.ServiceBusMessage) -> str:
    _a_resp = {
        "status": False,
        "miztiik_event_processed": False,
        "last_processed_on": None,
    }

    try:

        msg_body = msg.get_body().decode("utf-8")

        parsed_msg = json.loads(msg_body)
        start_time = datetime.datetime.fromisoformat(parsed_msg["ts"])
        processing_time = int((datetime.datetime.now() - start_time).total_seconds())

        enriched_msg = json.dumps(
            {
                "message_id": msg.message_id,
                "body": msg.get_body().decode("utf-8"),
                "content_type": msg.content_type,
                "delivery_count": msg.delivery_count,
                "expiration_time": (
                    msg.expiration_time.isoformat() if msg.expiration_time else None
                ),
                "label": msg.label,
                "partition_key": msg.partition_key,
                "reply_to": msg.reply_to,
                "reply_to_session_id": msg.reply_to_session_id,
                "scheduled_enqueue_time": (
                    msg.scheduled_enqueue_time.isoformat()
                    if msg.scheduled_enqueue_time
                    else None
                ),
                "session_id": msg.session_id,
                "time_to_live": msg.time_to_live,
                "to": msg.to,
                "user_properties": msg.user_properties,
                "event_type": msg.user_properties.get("event_type"),
                "processing_time": processing_time,
            },
            indent=4,
            sort_keys=True,
            default=str,
        )

        logging.info(f"{parsed_msg}")
        logging.info(f"recv_msg:\n {enriched_msg}")

        # write to blob
        write_to_blob(json.loads(msg_body))

        # write to cosmosdb
        write_to_cosmosdb(json.loads(msg_body))

        _a_resp["status"] = True
        _a_resp["miztiik_event_processed"] = True
        _a_resp["last_processed_on"] = datetime.datetime.now().isoformat()

        _a_resp["processing_time"] = processing_time

        logging.info(f"{json.dumps(_a_resp, indent=4, sort_keys=True, default=str)}")

    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")

    logging.info(json.dumps(_a_resp, indent=4, sort_keys=True, default=str))


if __name__ == "__main__":
    evnt_consumer()
