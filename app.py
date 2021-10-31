import json
from flask import Flask, request, jsonify
from config.conf import settings
from utils.pubsub import pubsub_message_to_dict
from jiffysql import Jiffy
app = Flask(__name__)


@app.route('/')
def home():
    return 'OK'


@app.route("/pubsub", methods=["POST"])
def pubsub():
    envelope = request.get_json()
    message_data, err_code = pubsub_message_to_dict(envelope)
    if err_code >= 300:
        return message_data, err_code
    data = json.loads(message_data["message"]["data"])
    jiffy = Jiffy(data)
    jiffy.run()
    return jsonify({'status': 'OK'}), 200


def run():
    app.run(host='0.0.0.0',
            port=settings['port'])


def debug():
    app.run(host='0.0.0.0',
            port='5000')


if __name__ == '__main__':
    run()
