import os
import datetime
import json
import logging
import random

from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.eventhub import EventHubProducerClient
from azure.eventhub import EventData
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient


class GlobalArgs:
    OWNER = "Mystique"
    VERSION = "2024-01-04"

    EVENT_HUB_FQDN = os.getenv("EVENT_HUB_FQDN")
    EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

    SA_NAME = os.getenv("SA_NAME")
    BLOB_SVC_ACCOUNT_URL = os.getenv("BLOB_SVC_ACCOUNT_URL")
    BLOB_NAME = os.getenv("BLOB_NAME", "store-events-blob-002")
    BLOB_PREFIX = "store_events/raw"

    COSMOS_DB_URL = os.getenv("COSMOS_DB_URL")
    COSMOS_DB_NAME = os.getenv(
        "COSMOS_DB_NAME", "open-telemetry-ne-db-account-002")
    COSMOS_DB_CONTAINER_NAME = os.getenv(
        "COSMOS_DB_CONTAINER_NAME", "store-backend-container-002")

    SVC_BUS_FQDN = os.getenv(
        "SVC_BUS_FQDN", "warehouse-q-svc-bus-ns-002.servicebus.windows.net")
    SVC_BUS_Q_NAME = os.getenv("SVC_BUS_Q_NAME", "warehouse-q-svc-bus-q-002")
    SVC_BUS_TOPIC_NAME = os.getenv("SVC_BUS_TOPIC_NAME")

    EVENT_HUB_FQDN = os.getenv("EVENT_HUB_FQDN")
    EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "store-events-stream-003")
    EVENT_HUB_SALE_EVENTS_CONSUMER_GROUP_NAME = os.getenv(
        "EVENT_HUB_SALE_EVENTS_CONSUMER_GROUP_NAME")


def _get_az_creds():
    try:
        azure_log_level = logging.getLogger("azure").setLevel(logging.ERROR)
        _az_creds = DefaultAzureCredential(
            logging_enable=False, logging=azure_log_level)
        return _az_creds
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")
        raise e


def write_to_blob(data: dict, blob_svc_attr: dict = None):
    try:
        blob_svc_attr = {
            "blob_svc_account_url": GlobalArgs.BLOB_SVC_ACCOUNT_URL,
            "blob_name": GlobalArgs.BLOB_NAME,
            "blob_prefix": GlobalArgs.BLOB_PREFIX,
            "container_prefix": data.get("event_type")
        }

        blob_svc_client = BlobServiceClient(
            blob_svc_attr["blob_svc_account_url"], credential=_get_az_creds())

        if blob_svc_attr.get('container_prefix'):
            blob_name = f"{blob_svc_attr['blob_prefix']}/event_type={blob_svc_attr['container_prefix']}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{datetime.datetime.now().strftime('%s%f')}.json"
        else:
            blob_name = f"{blob_svc_attr['blob_prefix']}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{datetime.datetime.now().strftime('%s%f')}.json"

        blob_client = blob_svc_client.get_blob_client(
            container=blob_svc_attr["blob_name"], blob=blob_name)

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
            url=db_attr["cosmos_db_url"], credential=_get_az_creds())
        db_client = cosmos_client.get_database_client(
            db_attr["cosmos_db_name"])
        db_container = db_client.get_container_client(
            db_attr["cosmos_db_container_name"])

        db_attr = {
            "cosmos_db_url": GlobalArgs.COSMOS_DB_URL,
            "cosmos_db_name": GlobalArgs.COSMOS_DB_NAME,
            "cosmos_db_container_name": GlobalArgs.COSMOS_DB_CONTAINER_NAME,
        }

        resp = db_container.create_item(body=data)
        logging.info(
            f"Document with id {data['id']} written to CosmosDB successfully")
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
        with ServiceBusClient(q_attr["svc_bus_fqdn"], credential=_get_az_creds()) as client:
            with client.get_queue_sender(q_attr["svc_bus_q_name"]) as sender:
                # Sending a single message
                msg_to_send = ServiceBusMessage(
                    json.dumps(data),
                    time_to_live=datetime.timedelta(days=1),
                    application_properties=msg_attr
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
        with ServiceBusClient(topic_attr["svc_bus_fqdn"], credential=_get_az_creds()) as client:
            with client.get_topic_sender(topic_name=topic_attr["svc_bus_topic_name"]) as sender:
                # Sending a single message
                msg_to_send = ServiceBusMessage(
                    json.dumps(data),
                    time_to_live=datetime.timedelta(days=1),
                    application_properties=msg_attr
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
            credential=_get_az_creds()
        )

        # Partition allocation strategy: Even partitions for inventory, odd partitions for sales
        inventory_partitions = [i for i in range(
            TOT_STREAM_PARTITIONS) if i % 2 == 0]
        sales_partitions = [i for i in range(
            TOT_STREAM_PARTITIONS) if i % 2 != 0]

        if msg_attr.get("event_type") == "sale_event":  # Send to sales partition
            STREAM_PARTITION_ID = str(random.choice(sales_partitions))
        elif msg_attr.get("event_type") == "inventory_event":  # Send to inventory partition
            STREAM_PARTITION_ID = str(random.choice(inventory_partitions))

        with producer:
            event_data_batch = producer.create_batch(
                partition_id=STREAM_PARTITION_ID)
            data_str = json.dumps(data)
            _evnt = EventData(data_str)
            _evnt.properties = msg_attr
            event_data_batch.add(_evnt)
            producer.send_batch(event_data_batch)
            logging.info(
                f"Sent messages with payload: {data_str} to partition:{TOT_STREAM_PARTITIONS}")
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
            storage_q_attr["storage_q_account_url"], credential=_get_az_creds())
        q_client = q_svc_client.get_queue_client(storage_q_attr["q_name"])
        resp = q_client.send_message(
            data, time_to_live=259200, visibility_timeout=60)
        logging.info(
            f"Message added to {storage_q_attr['q_name']} successfully")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")
        raise e
