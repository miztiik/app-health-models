from flask import Flask, request, jsonify, render_template, make_response

from store_events_producer import evnt_producer

from az_utils import (
    _get_az_creds,
    write_to_blob,
    write_to_cosmosdb,
    write_to_svc_bus_q,
    write_to_svc_bus_topic,
    write_to_event_hub,
    read_from_svc_bus_q,
)


from datetime import datetime
import socket
import os
import json

app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True


@app.route("/")
def index():
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _resp = make_response(
        render_template(
            "index.html",
            hostname=hostname,
            ip_address=ip_address,
            current_date=current_date,
        )
    )
    return _resp


@app.route("/event-producer", methods=["GET"])
def event_producer():
    resp_data = dict()
    events = evnt_producer(event_cnt=3)
    # resp_data["IDENTITY_ENDPOINT"] = os.getenv('IDENTITY_ENDPOINT')
    # resp_data["IDENTITY_HEADER"] = os.getenv('IDENTITY_HEADER')

    resp_data["events"] = events
    return jsonify(resp_data)


@app.route("/event-consumer", methods=["GET"])
def event_consumer():
    resp_data = dict()
    resp_data = read_from_svc_bus_q()
    return jsonify(resp_data)


@app.after_request
def add_custom_headers(response):
    response.headers["remote_addr"] = request.remote_addr
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["X-Response-Tag"] = (
        f"{socket.gethostname()} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    response.headers["X-Miztiik-Automation"] = "True"
    response.headers["X-Brand-Tag"] = "Empowering Innovations & Equitable Growth"
    return response


# Remove the following code block:
# if __name__ == '__main__':
#    app.run(host='0.0.0.0', port=80)


# Add the following code block:
if __name__ == "__main__":
    app.run()
