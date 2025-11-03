#!flask/bin/python
# Copyright 2014. Amazon Web Services, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import configparser
from dynamodb.connectionManager import ConnectionManager

from flask import Flask, jsonify

app = Flask(__name__)
connection_manager = None


@app.route("/")
def home():
    return jsonify({"message": "DynamoDB App Running Successfully ✅"})


@app.route("/games")
def get_games():
    table = connection_manager.getGamesTable()
    response = table.scan()
    data = response.get("Items", [])
    return jsonify(data)


def start_server(port):
    app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="Path to config.ini file", default=None)
    parser.add_argument("--mode", help="Mode: 'service' or 'local'", required=True)
    parser.add_argument("--endpoint", help="DynamoDB endpoint (optional)", default=None)
    parser.add_argument("--serverPort", help="Port for Flask server", default=5000)
    parser.add_argument("--useMetadata", help="Use EC2 instance metadata", action="store_true")

    args = parser.parse_args()

    config = None
    if args.config:
        config = configparser.ConfigParser()
        config.read(args.config)

    connection_manager = ConnectionManager(
        mode=args.mode,
        config=config,
        endpoint=args.endpoint,
        port=None,
        use_instance_meta
