#!/usr/bin/env python3
from datetime import datetime
from flask import Flask, jsonify, request
from auction_db import AuctionDbEndpoint


# To allow patching and mocking
def get_time():
    return datetime.now()

def get_app(database_file, logger, endpoint_name):
    app = Flask(__name__)

    @app.route(f"/{endpoint_name}/set",methods=["POST"])
    def submit_orders():
        incoming = request.get_json()
        key = incoming["key"]
        ts = get_time()
        orders = [o| {"timestamp":ts} for o in incoming["orders"]]

        auction_db = AuctionDbEndpoint(database_file,logger=logger)
        accepted,message = auction_db.write_orders(key,orders)

        return jsonify({"accepted":accepted,"message":message})

    @app.route(f"/{endpoint_name}/get",methods=["GET"])
    def get_orders():
        bid_db = AuctionDbEndpoint(database_file,logger=logger)
        applying_date = datetime.strptime(request.args["applying_date"],"%Y-%m-%d").date()
        key = request.args["key"]
        hour_ID = request.args["hour_ID"] if "hour_ID" in request.args else None

        orders = bid_db.read_orders(key, applying_date, hour_ID)

        return jsonify(orders)

    return app
