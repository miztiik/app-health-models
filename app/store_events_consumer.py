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
    VERSION = "2024-04-14"


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
            }
        )

        logging.info(f"{json.dumps(msg_body, indent=4)}")
        logging.info(f"recv_msg:\n {enriched_msg}")

        # write to blob
        write_to_blob(json.loads(msg_body))

        # write to cosmosdb
        write_to_cosmosdb(json.loads(msg_body))

        _a_resp["status"] = True
        _a_resp["miztiik_event_processed"] = True
        _a_resp["last_processed_on"] = datetime.datetime.now().isoformat()
        logging.info(f"{json.dumps(_a_resp)}")

    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")

    logging.info(json.dumps(_a_resp, indent=4))


def process_event_hub_evnts(event: func.EventHubEvent) -> str:
    _a_resp = {
        "status": False,
        "miztiik_event_processed": False,
        "last_processed_on": None,
    }

    try:
        recv_body = json.loads(event.get_body().decode("UTF-8"))
        recv_body["event_type"] = event.metadata["Properties"].get("event_type")

        # Metadata
        for key in event.metadata:
            logging.info(f"Metadata: {key} = {event.metadata[key]}")

        result = json.dumps(
            {
                "recv_body": recv_body,
                "recv_body_type": str(recv_body),
                "enqueued_time_utc": str(event.enqueued_time),
                "seq_no": event.sequence_number,
                "offset": event.offset,
                "event_property": event.metadata["Properties"],
                "metadata": event.metadata,
                "event_type": event.metadata["Properties"].get("event_type"),
                "event_from_partition": event.metadata["PartitionContext"].get(
                    "PartitionId"
                ),
            }
        )

        logging.info(f"recv_event:\n {result}")

        # write to blob
        write_to_blob(recv_body)

        # write to cosmosdb
        write_to_cosmosdb(recv_body)

        _a_resp["status"] = True
        _a_resp["miztiik_event_processed"] = True
        _a_resp["last_processed_on"] = datetime.datetime.now().isoformat()
        logging.info(f"{json.dumps(_a_resp)}")

    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")

    logging.info(json.dumps(_a_resp, indent=4))


if __name__ == "__main__":
    evnt_consumer()
