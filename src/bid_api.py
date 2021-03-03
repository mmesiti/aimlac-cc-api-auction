#!/usr/bin/env python3
#TODO
from flask import Flask, jsonify, request
from auction_db import AuctionDbEndpoint

def get_app(database_file, logger, endpoint_name):
    app = Flask(__name__)

    @app.route(f"/{endpoint_name}",methods=["POST"])
    def get_bid():
        bid_db = AuctionDbEndpoint(database_file,logger=logger)
        bid = dict(
            apikey = requests.args.get("apikey"),
            ts = requests.args.get("timestamp"),
            buy_or_sell = requests.args.get("type"),
            volume_kwh = requests.args.get("volume"),
            price_pounds = requests.args.get("price"),
        )
        bid_id = bid_db.add(bid)
        return jsonify({"bid_id":bid_id})

    @app.route(f"/{endpoint_name}/<bid_id>",methods=["GET"])
    def manage_bid(bid_id):
        bid_db = AuctionDbEndpoint(database_file,logger=logger)
        if request.method == "GET":
            bid = bid_db.get(bid_id)
            return jsonify(bid)

    return app
