import os
import time
import datetime
import isodate
import json
import logging
import random

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.eventhub import EventHubProducerClient
from azure.eventhub import EventData
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient


class GlobalArgs:
    OWNER = "Mystique"
    VERSION = "2024-04-17"
    TRIGGER_RANDOM_FAILURES = os.getenv("TRIGGER_RANDOM_FAILURES", True)
    WAIT_SECS_BETWEEN_MSGS = int(os.getenv("WAIT_SECS_BETWEEN_MSGS", 2))
    MAX_MSGS_TO_PROCESS = int(os.getenv("MAX_MSGS_TO_PROCESS", 5))
    MAX_BACKOFF_SECS = int(os.getenv("MAX_BACKOFF_SECS", 30))

    EVENT_HUB_FQDN = os.getenv("EVENT_HUB_FQDN")
    EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

    SA_NAME = os.getenv("SA_NAME")
    BLOB_SVC_ACCOUNT_URL = os.getenv("BLOB_SVC_ACCOUNT_URL")
    BLOB_NAME = os.getenv("BLOB_NAME", "store-events-blob-002")
    BLOB_PREFIX = "store_events/raw"

    COSMOS_DB_URL = os.getenv("COSMOS_DB_URL")
    COSMOS_DB_NAME = os.getenv("COSMOS_DB_NAME", "open-telemetry-ne-db-account-002")
    COSMOS_DB_CONTAINER_NAME = os.getenv(
        "COSMOS_DB_CONTAINER_NAME", "store-backend-container-002"
    )

    SVC_BUS_FQDN = os.getenv(
        "SVC_BUS_FQDN", "warehouse-q-svc-bus-ns-002.servicebus.windows.net"
    )
    SVC_BUS_Q_NAME = os.getenv("SVC_BUS_Q_NAME", "warehouse-q-svc-bus-q-002")
    SVC_BUS_TOPIC_NAME = os.getenv("SVC_BUS_TOPIC_NAME")

    EVENT_HUB_FQDN = os.getenv("EVENT_HUB_FQDN")
    EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "store-events-stream-003")
    EVENT_HUB_SALE_EVENTS_CONSUMER_GROUP_NAME = os.getenv(
        "EVENT_HUB_SALE_EVENTS_CONSUMER_GROUP_NAME"
    )


def _get_az_creds():
    try:
        azure_log_level = logging.getLogger("azure").setLevel(logging.ERROR)
        _az_creds = DefaultAzureCredential(
            logging_enable=False, logging=azure_log_level
        )
        return _az_creds
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")
        raise e


############################################
#           PRODUCER UTILITIES             #
############################################


def write_to_blob(data: dict, blob_svc_attr: dict = None):
    try:
        blob_svc_attr = {
            "blob_svc_account_url": GlobalArgs.BLOB_SVC_ACCOUNT_URL,
            "blob_name": GlobalArgs.BLOB_NAME,
            "blob_prefix": GlobalArgs.BLOB_PREFIX,
            "container_prefix": data.get("event_type"),
        }

        blob_svc_client = BlobServiceClient(
            blob_svc_attr["blob_svc_account_url"], credential=_get_az_creds()
        )

        if blob_svc_attr.get("container_prefix"):
            blob_name = f"{blob_svc_attr['blob_prefix']}/event_type={blob_svc_attr['container_prefix']}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{datetime.datetime.now().strftime('%s%f')}.json"
        else:
            blob_name = f"{blob_svc_attr['blob_prefix']}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{datetime.datetime.now().strftime('%s%f')}.json"

        blob_client = blob_svc_client.get_blob_client(
            container=blob_svc_attr["blob_name"], blob=blob_name
        )

        # if blob_client.exists():
        #     blob_client.delete_blob()
        #     logging.debug(
        #         f"Blob {blob_name} already exists. Deleted the file.")

        resp = blob_client.upload_blob(json.dumps(data).encode("UTF-8"))

        logging.info(f"Blob {blob_name} uploaded successfully")
        logging.debug(f"{resp}")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")
        raise e


def write_to_cosmosdb(data: dict, db_attr: dict = None):
    try:
        db_attr = {
            "cosmos_db_url": GlobalArgs.COSMOS_DB_URL,
            "cosmos_db_name": GlobalArgs.COSMOS_DB_NAME,
            "cosmos_db_container_name": GlobalArgs.COSMOS_DB_CONTAINER_NAME,
        }
        cosmos_client = CosmosClient(
            url=db_attr["cosmos_db_url"], credential=_get_az_creds()
        )
        db_client = cosmos_client.get_database_client(db_attr["cosmos_db_name"])
        db_container = db_client.get_container_client(
            db_attr["cosmos_db_container_name"]
        )

        db_attr = {
            "cosmos_db_url": GlobalArgs.COSMOS_DB_URL,
            "cosmos_db_name": GlobalArgs.COSMOS_DB_NAME,
            "cosmos_db_container_name": GlobalArgs.COSMOS_DB_CONTAINER_NAME,
        }

        resp = db_container.create_item(body=data)
        logging.info(f"Document with id {data['id']} written to CosmosDB successfully")
        logging.debug(f"{resp}")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")
        raise e


def write_to_svc_bus_q(data, msg_attr, q_attr: dict = None):
    try:
        q_attr = {
            "svc_bus_fqdn": GlobalArgs.SVC_BUS_FQDN,
            "svc_bus_q_name": GlobalArgs.SVC_BUS_Q_NAME,
        }
        with ServiceBusClient(
            q_attr["svc_bus_fqdn"], credential=_get_az_creds()
        ) as client:
            with client.get_queue_sender(q_attr["svc_bus_q_name"]) as sender:
                # Sending a single message
                msg_to_send = ServiceBusMessage(
                    json.dumps(data),
                    time_to_live=datetime.timedelta(days=1),
                    application_properties=msg_attr,
                )

                _r = sender.send_messages(msg_to_send)
                logging.debug(f"Message sent: {json.dumps(_r)}")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")
        raise e


def write_to_svc_bus_topic(data, msg_attr, topic_attr: dict = None):
    try:
        topic_attr = {
            "svc_bus_fqdn": GlobalArgs.SVC_BUS_FQDN,
            "svc_bus_topic_name": GlobalArgs.SVC_BUS_TOPIC_NAME,
        }
        with ServiceBusClient(
            topic_attr["svc_bus_fqdn"], credential=_get_az_creds()
        ) as client:
            with client.get_topic_sender(
                topic_name=topic_attr["svc_bus_topic_name"]
            ) as sender:
                # Sending a single message
                msg_to_send = ServiceBusMessage(
                    json.dumps(data),
                    time_to_live=datetime.timedelta(days=1),
                    application_properties=msg_attr,
                )

                _r = sender.send_messages(msg_to_send)
                logging.info(f"Event written to topic Successfully")
                logging.debug(f"Message sent: {json.dumps(_r)}")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")
        raise e


def write_to_event_hub(data, msg_attr, event_hub_attr: dict = None):
    try:
        TOT_STREAM_PARTITIONS = 4
        STREAM_PARTITION_ID = 0
        event_hub_attr = {
            "event_hub_fqdn": GlobalArgs.EVENT_HUB_FQDN,
            "event_hub_name": GlobalArgs.EVENT_HUB_NAME,
        }

        producer = EventHubProducerClient(
            fully_qualified_namespace=event_hub_attr["event_hub_fqdn"],
            eventhub_name=event_hub_attr["event_hub_name"],
            credential=_get_az_creds(),
        )

        # Partition allocation strategy: Even partitions for inventory, odd partitions for sales
        inventory_partitions = [i for i in range(TOT_STREAM_PARTITIONS) if i % 2 == 0]
        sales_partitions = [i for i in range(TOT_STREAM_PARTITIONS) if i % 2 != 0]

        if msg_attr.get("event_type") == "sale_event":  # Send to sales partition
            STREAM_PARTITION_ID = str(random.choice(sales_partitions))
        elif (
            msg_attr.get("event_type") == "inventory_event"
        ):  # Send to inventory partition
            STREAM_PARTITION_ID = str(random.choice(inventory_partitions))

        with producer:
            event_data_batch = producer.create_batch(partition_id=STREAM_PARTITION_ID)
            data_str = json.dumps(data)
            _evnt = EventData(data_str)
            _evnt.properties = msg_attr
            event_data_batch.add(_evnt)
            producer.send_batch(event_data_batch)
            logging.info(
                f"Sent messages with payload: {data_str} to partition:{TOT_STREAM_PARTITIONS}"
            )
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")
        raise e


def write_to_storage_q(data: dict, storage_q_attr: dict = None):
    try:
        storage_q_attr = {
            "storage_q_account_url": GlobalArgs.STORAGE_Q_ACCOUNT_URL,
            "q_name": GlobalArgs.Q_NAME,
        }

        q_svc_client = QueueServiceClient(
            storage_q_attr["storage_q_account_url"], credential=_get_az_creds()
        )
        q_client = q_svc_client.get_queue_client(storage_q_attr["q_name"])
        resp = q_client.send_message(data, time_to_live=259200, visibility_timeout=60)
        logging.info(f"Message added to {storage_q_attr['q_name']} successfully")
        return resp
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")
        raise e


############################################
#           CONSUMER UTILITIES             #
############################################


def read_from_svc_bus_q(max_msgs=GlobalArgs.MAX_MSGS_TO_PROCESS):
    _r = {
        "status": False,
        "event_process_duration": 0,
        "max_msg_count": max_msgs,
        "exit_msg": "",
    }
    backoff_time = 1
    # maximum backoff time in seconds
    max_backoff_secs = GlobalArgs.MAX_BACKOFF_SECS
    success_msg_count = 0
    retrieved_msg_count = 0

    # Start timing the event generation
    event_process_start_time = time.time()

    # Setup up Azure Credentials
    azure_log_level = logging.getLogger("azure").setLevel(logging.ERROR)
    default_credential = DefaultAzureCredential(
        logging_enable=False, logging=azure_log_level
    )

    with ServiceBusClient(
        GlobalArgs.SVC_BUS_FQDN, credential=default_credential
    ) as client:
        with client.get_queue_receiver(GlobalArgs.SVC_BUS_Q_NAME) as receiver:

            while success_msg_count < max_msgs:
                try:
                    recv_msgs = receiver.receive_messages(
                        max_message_count=1, max_wait_time=5
                    )
                    if not recv_msgs:
                        if backoff_time >= max_backoff_secs:
                            print("Maximum backoff time reached. Exiting.")
                            logging.info(
                                f"Current backoff time:{backoff_time} exceeds max backoff timereached. Exiting."
                            )
                            _r["exit_msg"] = (
                                f"Current backoff time:{backoff_time} exceeds max backoff timereached. Exiting."
                            )
                            break  # Exit the loop if max backoff is reached
                        logging.info(
                            f"No messages received. Current backoff time: {backoff_time} seconds. Time to reset: {max_backoff_secs - backoff_time} seconds."
                        )
                        print(
                            f"No messages received. Current backoff time: {backoff_time} seconds. Time to reset: {max_backoff_secs - backoff_time} seconds."
                        )
                        time.sleep(backoff_time)
                        # exponential backoff with maximum
                        backoff_time = min(backoff_time * 2, max_backoff_secs)
                    else:
                        backoff_time = 1  # reset backoff time on successful receive
                        retrieved_msg_count += 1

                    recv_event = {}
                    for msg in recv_msgs:
                        recv_event["id"] = msg.message_id
                        recv_event["body"] = json.loads(str(msg))
                        recv_event["content_type"] = msg.content_type
                        recv_event["delivery_count"] = msg.delivery_count
                        recv_event["partition_key"] = msg.partition_key
                        recv_event["reply_to"] = msg.reply_to
                        recv_event["reply_to_session_id"] = msg.reply_to_session_id
                        recv_event["session_id"] = msg.session_id
                        recv_event["time_to_live"] = isodate.duration_isoformat(
                            msg.time_to_live
                        )
                        recv_event["to"] = msg.to
                        recv_event["user_properties"] = {
                            key.decode(): value.decode()
                            for key, value in msg.application_properties.items()
                        }
                        recv_event["event_type"] = recv_event["user_properties"].get(
                            "event_type"
                        )

                        # Check for random failures
                        if GlobalArgs.TRIGGER_RANDOM_FAILURES:
                            if recv_event["body"].get("store_id") is None:
                                print("Random failure triggered, 'store_id' is missing")
                                logging.error(
                                    "Random failure triggered, 'store_id' is missing"
                                )
                                raise Exception("'store_id' is missing")

                        start_time = datetime.datetime.fromisoformat(
                            recv_event["body"]["ts"]
                        )
                        processing_time = int(
                            (datetime.datetime.now() - start_time).total_seconds()
                        )
                        recv_event["processing_time"] = processing_time

                        print(
                            f"Received: {success_msg_count} of {max_msgs} messages. Current backoff time: {backoff_time} seconds. Time to reset: {max_backoff_secs - backoff_time} seconds."
                        )
                        print(f"{recv_event}")

                        # Write to blob
                        write_to_blob(recv_event)

                        # Write to Cosmos DB
                        write_to_cosmosdb(recv_event)

                        receiver.complete_message(msg)
                        success_msg_count += 1
                except Exception as e:
                    print(f"Error receiving message: {e}")
                    logging.error(f"Error receiving message: {e}")
    event_process_end_time = time.time()  # Stop timing the event generation
    event_process_duration = (
        event_process_end_time - event_process_start_time
    )  # Calculate the duration
    _r["status"] = True
    _r["event_process_duration"] = round(event_process_duration)
    _r["retrieved_msg_count"] = retrieved_msg_count
    _r["success_msg_count"] = success_msg_count
    print(
        f"Received: {success_msg_count} of {max_msgs} messages. Max msg count or Max backoff {backoff_time} reached, exiting"
    )
    return _r


def process_q_msg(msg: func.ServiceBusMessage) -> str:
    _a_resp = {
        "status": False,
        "miztiik_event_processed": False,
        "last_processed_on": None,
    }

    try:

        msg_body = msg.get_body().decode("utf-8")

        parsed_msg = json.loads(msg_body)
        # Check for random failures
        if GlobalArgs.TRIGGER_RANDOM_FAILURES:
            if parsed_msg.get("store_id") is None:
                print("Random failure triggered, 'store_id' is missing")
                logging.error("Random failure triggered, 'store_id' is missing")
                raise Exception("'store_id' is missing")

        # Calculate processing time
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
            }
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
