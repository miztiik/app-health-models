from flask import Flask, render_template
from datetime import datetime
import socket

app = Flask(__name__)


@app.route('/')
def index():
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return render_template('index.html', hostname=hostname, ip_address=ip_address, current_date=current_date)

# Remove the following code block:
# if __name__ == '__main__':
#    app.run(host='0.0.0.0', port=80)


# Add the following code block:
if __name__ == '__main__':
    app.run()
