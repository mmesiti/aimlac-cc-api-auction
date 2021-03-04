#!/usr/bin/env python3
from bid_api import get_app
import converter
from fixtures import *
from fixtures import (keys,#for legibility
                      orders,
                      db_withapidata_and_keys,
                      pandas_orders)

from datetime import date,datetime
from unittest.mock import patch
import pytest

endpoint_name = "bids"

@pytest.fixture
def client(db_withapidata_and_keys):
    app = get_app(db_withapidata_and_keys.filename,None,endpoint_name)

    with app.test_client() as client:
        yield client

def test_push_single_order(client):
    order = {"applying_date" : "2021-03-05",
             "hour_ID": 15,
             "type" : "BUY",
             "volume": "0.60",
             "price":"30"}

    r = client.post(f"/{endpoint_name}/set",
                       json = {"key" : keys[1], "orders": [order]})

    d = r.json
    assert d["accepted"] == 1
    assert d["message"] == ''

def test_push_orders(client,pandas_orders):
    fixed_orders = converter.pandas_orders_to_records(pandas_orders)
    with patch("bid_api.get_time",return_value = datetime(2021,2,18,8,20)):
        r = client.post(f"/{endpoint_name}/set",
                       json = {"key" : keys[1], "orders": fixed_orders})
    d = r.json
    assert d["accepted"] == len(pandas_orders)

def test_push_orders_late(client,pandas_orders):
    fixed_orders = converter.pandas_orders_to_records(pandas_orders)
    with patch("bid_api.get_time",return_value = datetime(2021,2,18,9,0,1)):
        r = client.post(f"/{endpoint_name}/set",
                       json = {"key" : keys[1], "orders": fixed_orders})
    d = r.json
    assert d["accepted"] == len(pandas_orders)/2
    assert "time limit" in d["message"]

def test_get_orders_wrong_key(client):
    rv = client.get(f"/{endpoint_name}/get",
                    query_string = dict(key="WRONG",
                                        applying_date=date(2021,2,19).isoformat()))
    assert rv.json == None

def test_get_orders_wrong_empty(client):
    rv = client.get(f"/{endpoint_name}/get",
                    query_string = dict(key=keys[1],
                                        applying_date=date(2021,2,19).isoformat()))
    assert rv.json == []

def test_push_and_get_orders_wrong_key(client,pandas_orders):
    fixed_orders = converter.pandas_orders_to_records(pandas_orders)
    with patch("bid_api.get_time",return_value = datetime(2021,2,18,8,20)):
        r = client.post(f"/{endpoint_name}/set",
                       json = {"key" : keys[1], "orders": fixed_orders})

    rv = client.get(f"/{endpoint_name}/get",
                    query_string = dict(key=keys[0],
                                        applying_date=date(2021,2,19).isoformat()))

    assert rv.json == []

def test_push_and_get_orders_ok(client,pandas_orders):
    fixed_orders = converter.pandas_orders_to_records(pandas_orders)
    with patch("bid_api.get_time",return_value = datetime(2021,2,18,8,20)):
        r = client.post(f"/{endpoint_name}/set",
                       json = {"key" : keys[1], "orders": fixed_orders})

    rv = client.get(f"/{endpoint_name}/get",
                    query_string = dict(key=keys[1],
                                        applying_date=date(2021,2,19).isoformat()))

    assert len(rv.json) == 48
    assert all("accepted" in r for r in rv.json)
