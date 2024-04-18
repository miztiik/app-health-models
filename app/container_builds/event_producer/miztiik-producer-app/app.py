from flask import Flask, jsonify, render_template

from event_producer import evnt_producer
import json
from datetime import datetime
import socket

app = Flask(__name__)


@app.route('/')
def index():
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return render_template('index.html', hostname=hostname, ip_address=ip_address, current_date=current_date)


@app.route('/event-producer', methods=['GET'])
def event_producer():
    events = None
    events = evnt_producer()
    return jsonify(events)

# Remove the following code block:
# if __name__ == '__main__':
#    app.run(host='0.0.0.0', port=80)


# Add the following code block:
if __name__ == '__main__':
    app.run()
