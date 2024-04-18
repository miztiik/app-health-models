import azure.functions as func
import os
import logging
import json
import datetime

from store_events_producer import evnt_producer
from store_events_consumer import process_q_msg
from az_utils import _get_az_creds, write_to_blob, write_to_cosmosdb, write_to_svc_bus_q, write_to_svc_bus_topic, write_to_event_hub


from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from azure.monitor.opentelemetry.exporter import AzureMonitorTraceExporter


app = func.FunctionApp()


class GlobalArgs:
    OWNER = "Mystique"
    VERSION = "2024-01-04"


def configure_tracer(svc_name):
    trace.set_tracer_provider(TracerProvider(
        resource=Resource.create({"service.name": svc_name})))
    tracer = trace.get_tracer(__name__)

    # This is the exporter that sends data to Application Insights
    span_exporter = AzureMonitorTraceExporter(
        connection_string=os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
    )
    span_processor = BatchSpanProcessor(span_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)
    return tracer


@app.function_name(name="greeter")
# Run midnight everyday
# @app.schedule(schedule="0 0 * * * *", arg_name="timer", run_on_startup=True)
@app.route(route="miztiik-automation/greeter", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def http_greeter(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    _d = {
        "miztiik_event_processed": True,
        "msg": "",
        "processed_at": f"{datetime.datetime.now()}",
        # "IDENTITY_ENDPOINT": os.getenv("IDENTITY_ENDPOINT"),
        # "IDENTITY_HEADER": os.getenv("IDENTITY_HEADER")
    }

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        # return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
        _d["msg"] = f"Hello, {name}. This HTTP triggered function executed successfully."
        return func.HttpResponse(
            f"{json.dumps(_d, indent=4)}",
            status_code=200
        )
    else:
        return func.HttpResponse(
            f"{json.dumps(_d, indent=4)}",
            status_code=200
        )


@app.function_name(name="store_events_producer")
@app.route(route="miztiik_automation/store_events_producer", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def store_events_producer(req: func.HttpRequest, context) -> func.HttpResponse:
    recv_cnt = 0
    _d = {
        "miztiik_event_processed": False,
        "msg": "",
        "event_count": 1,
    }

    try:
        try:
            recv_cnt = req.params.get("count")
            if recv_cnt:
                _d["event_count"] = int(recv_cnt)
            logging.debug(f"got from params: {recv_cnt}")
        except ValueError:
            pass

        # Setting up tracing
        producer_tracer = configure_tracer(context.function_name)

        with producer_tracer.start_as_current_span(f"miztiik-event-producer-trace") as span:
            span.set_attribute("event_count", _d["event_count"])
            ###############################################################
            #                       Generate Events                       #
            ###############################################################
            resp = evnt_producer(_d["event_count"])
            _d["resp"] = resp

        if resp.get("status"):
            _d["miztiik_event_processed"] = True
            # _d["headers"] = dict(req.headers)
            _d["msg"] = f"Generated {resp.get('tot_msgs')} messages"
            _d["last_processed_on"] = datetime.datetime.now().isoformat()
        logging.info(f"{json.dumps(_d)}")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")
        _d["msg"] = f"ERROR:{str(e)}"

    return func.HttpResponse(
        f"{json.dumps(_d, indent=4)}",
        status_code=200
    )


@app.function_name(name="store_events_consumer")
@app.service_bus_topic_trigger(
    arg_name="msg",
    topic_name=os.getenv("SVC_BUS_TOPIC_NAME"),
    connection="SVC_BUS_CONNECTION",
    subscription_name=os.getenv("SALES_EVENTS_SUBSCRIPTION_NAME")
)
def store_events_consumer(msg: func.ServiceBusMessage, context) -> str:
    __resp = {
        "status": False
    }
    try:
        # Setting up tracing
        consumer_tracer = configure_tracer(context.function_name)
        with consumer_tracer.start_as_current_span(f"miztiik-event-consumer-trace") as span:
            ###############################################################
            #                       Process Events                        #
            ###############################################################
            __resp = process_q_msg(msg)
            logging.info(f"{json.dumps(__resp)}")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")
        raise e

    logging.info(json.dumps(__resp, indent=4))
