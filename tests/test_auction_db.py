#!/usr/bin/env python3

import auction_db
from fixtures import (
    orders,
    keys,
    db_endpoint,
    db_wkeys,
    db_withapidata,
    db_worders,
    db_withapidata_and_keys,
    db_complete,
    market_index,
    imbalance_prices,
    pandas_orders,
)

from datetime import datetime, date
import pandas as pd
import pytest


def test_init(db_endpoint):
    res = db_endpoint.execute('''
    SELECT *
    FROM sqlite_master;
    ''').fetchall()

    assert len(res) == (
        len(db_endpoint.tables) +
        1  # because of the unique constraint in the "keys" table
        + 2  # because of the unique constraints on the
        # market_index and imbalance_market tables
    )
    assert "sqlite_sequence" in [r[1] for r in res]


def test_write_key(db_endpoint):
    key = "AAAAAAA"
    db_endpoint.write_key(key)
    key_id = db_endpoint.find_key_id(key)
    assert key_id == 1

    key = "BBBBBB"
    db_endpoint.write_key(key)
    key_id = db_endpoint.find_key_id(key)
    assert key_id == 2


def test_write_key_again(db_endpoint):
    key = "AAAAAAA"
    db_endpoint.write_key(key)
    key_id = db_endpoint.find_key_id(key)
    assert key_id == 1

    db_endpoint.write_key(key)  # this should not do anything

    nkeys, = db_endpoint.execute('''
    SELECT COUNT(*)
    FROM keys;''').fetchone()

    assert nkeys == 1


def test_write_orders(db_wkeys):
    key = keys[1]
    nwritten, message = db_wkeys.write_orders(key, orders)

    n, = db_wkeys.execute('''
    SELECT COUNT(*)
    FROM orders;''').fetchone()

    assert nwritten == n
    assert nwritten == len(orders)


def test_order_after_9_rejected(db_wkeys):

    orders = [
        {
            "timestamp": datetime(2021, 3, 4, 10),  # This won't go through
            "applying_date": date(2021, 3, 5),
            "hour_ID": 15,
            "type": "BUY",
            "volume": "0.60",
            "price": "30"
        },
        {
            "timestamp": datetime(2021, 3, 4, 10),
            "applying_date": date(2021, 3, 6),
            "hour_ID": 15,
            "type": "BUY",
            "volume": "0.60",
            "price": "30"
        },
    ]

    key = keys[1]
    nwritten, message = db_wkeys.write_orders(key, orders)
    n, = db_wkeys.execute('''
    SELECT COUNT(*)
    FROM orders;''').fetchone()

    assert nwritten == 1
    assert nwritten == n
    assert "rejected" in message.lower()


def test_write_wrong_hour():
    order = {
        "timestamp": datetime(2021, 3, 4, 8),
        "applying_date": date(2021, 3, 5),
        "hour_ID": 1,
        "type": "SELL",
        "volume": "0.10",
        "price": "80"
    }

    assert auction_db.AuctionDbEndpoint.validate_order(order)
    order["hour_ID"] = 0
    assert not auction_db.AuctionDbEndpoint.validate_order(order)
    order["hour_ID"] = 25
    assert not auction_db.AuctionDbEndpoint.validate_order(order)


def test_write_wrong_type():
    order = {
        "timestamp": datetime(2021, 3, 4, 8),
        "applying_date": date(2021, 3, 5),
        "hour_ID": 1,
        "type": "SELL",
        "volume": "0.10",
        "price": "80"
    }

    assert auction_db.AuctionDbEndpoint.validate_order(order)
    order["type"] = 'buy'
    assert auction_db.AuctionDbEndpoint.validate_order(order)
    order["type"] = 'french fries'
    assert not auction_db.AuctionDbEndpoint.validate_order(order)


def test_sanify_order_type_allcaps():
    order = {
        "timestamp": datetime(2021, 3, 4, 8),
        "applying_date": date(2021, 3, 5),
        "hour_ID": 1,
        "type": "sell",
        "volume": "0.10",
        "price": "80"
    }

    processed = auction_db.AuctionDbEndpoint.sanify_order(1, order)

    assert "SELL" == processed["type"]


def test_read_order_slot_wrong_key(db_worders):
    testdate = date(2021, 3, 5)
    period = 17

    res = db_worders.read_orders(
        key=keys[0],  # the wrong one
        applying_date=testdate,
        period=period)

    assert len(res) == 0


def test_read_order_slot_inexistent_key(db_worders):
    testdate = date(2021, 3, 5)
    period = 17

    res = db_worders.read_orders(key="WRONG",
                                 applying_date=testdate,
                                 period=period)

    assert res is None


def test_read_orders(db_worders):
    testdate = date(2021, 3, 5)
    period = 17

    res = db_worders.read_orders(key=keys[1],
                                 applying_date=testdate,
                                 period=period)

    assert len(res) == 2


def test_read_orders_wholeday(db_worders):
    testdate = date(2021, 3, 5)

    res = db_worders.read_orders(key=keys[1], applying_date=testdate)

    assert len(res) == 4


def test_write_market_index(db_endpoint, market_index):
    db_endpoint.write_market_index(market_index)

    lines, = db_endpoint.execute('''
    SELECT COUNT(*)
    FROM market_index;''').fetchone()

    assert lines == len(market_index)


def test_write_market_index_twice(db_endpoint, market_index):
    db_endpoint.write_market_index(market_index)
    db_endpoint.write_market_index(market_index)

    lines, = db_endpoint.execute('''
    SELECT COUNT(*)
    FROM market_index;''').fetchone()

    assert lines == len(market_index)


def test_write_imbalance_prices(db_endpoint, imbalance_prices):
    db_endpoint.write_imbalance_prices(imbalance_prices)

    lines, = db_endpoint.execute('''
    SELECT COUNT(*)
    FROM imbalance_prices;''').fetchone()

    assert lines == len(imbalance_prices)


def test_write_imbalance_prices_twice(db_endpoint, imbalance_prices):
    db_endpoint.write_imbalance_prices(imbalance_prices)
    db_endpoint.write_imbalance_prices(imbalance_prices)

    lines, = db_endpoint.execute('''
    SELECT COUNT(*)
    FROM imbalance_prices;''').fetchone()

    assert lines == len(imbalance_prices)


def test_read_imbalance_prices_all(db_withapidata, imbalance_prices):
    start_date = date(2021, 2, 18)
    end_date = date(2021, 2, 20)
    read_data = db_withapidata.read_imbalance_prices(start_date, end_date)

    assert len(read_data) == len(imbalance_prices)


def test_read_imbalance_prices_none(db_withapidata):
    start_date = date(2021, 2, 17)
    end_date = date(2021, 2, 18)
    read_data = db_withapidata.read_imbalance_prices(start_date, end_date)

    assert len(read_data) == 0


def test_read_market_index_all(db_withapidata, market_index):
    start_date = date(2021, 2, 18)
    end_date = date(2021, 2, 21)
    read_data = db_withapidata.read_market_index(start_date, end_date)

    assert len(read_data) == len(market_index)


def test_read_market_index_none(db_withapidata, market_index):
    start_date = date(2021, 2, 17)
    end_date = date(2021, 2, 18)
    read_data = db_withapidata.read_market_index(start_date, end_date)

    assert len(read_data) == 0


def test_write_orders_pandas(db_wkeys, pandas_orders):
    key = keys[0]

    n, message = db_wkeys.write_orders_pandas(key, pandas_orders)
    assert n == len(pandas_orders)


def test_read_orders_accept_reject(db_complete, pandas_orders, market_index):

    applying_date = date(2021, 2, 19)
    res = db_complete.read_orders(key=keys[1], applying_date=applying_date)

    mid19 = (market_index.loc[market_index["Settlement Date"] == applying_date,
                              ["Settlement Period", "Price"]].rename(
                                  mapper={
                                      "Price": "market_price",
                                      "Settlement Period": "period"
                                  },
                                  axis="columns"))

    ord19 = (pandas_orders.loc[pandas_orders["applying_date"] == applying_date,
                               ["hour_ID", "price", "type"]].rename(
                                   mapper={
                                       "price": "order_price",
                                       "hour_ID": "period"
                                   },
                                   axis="columns"))

    ords = pd.merge(mid19, ord19, on="period")
    ords["accepted"] = (((ords["market_price"] >= ords["order_price"]) &
                         (ords["type"] == "SELL"))
                        | ((ords["market_price"] <= ords["order_price"]) &
                           (ords["type"] == "BUY")))

    res = pd.DataFrame(res)
    assert len(res) == len(ords)
    assert (res["accepted"] == ords["accepted"]).all()


def test_read_orders_unknown_state(db_complete, pandas_orders, market_index):

    ords = [o for o in orders if o["applying_date"] == date(2021, 3, 5)]
    nwritten, message = db_complete.write_orders(keys[1], ords)

    res = db_complete.read_orders(key=keys[1], applying_date=date(2021, 3, 5))

    res = pd.DataFrame(res)

    assert len(ords) == len(res)
    assert pd.isna(res["accepted"].iloc[-len(orders):]).all()
