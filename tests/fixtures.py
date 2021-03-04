#!/usr/bin/env python3

import auction_db
import query_bmrs as bmrs

import pytest
from datetime import datetime,date,time
import pandas as pd
import os

@pytest.fixture
def db_endpoint():
    yield auction_db.AuctionDbEndpoint(filename = "test.db",
                                       logger = None,
                                       order_deadline = time(9))
    os.remove("test.db")

keys = ["AAAAAAA","BBBBBB","CCCCCCC"]
@pytest.fixture
def db_wkeys(db_endpoint):
    for key in keys:
        db_endpoint.write_key(key)
    return db_endpoint

orders =[
    {"timestamp" : datetime(2021,3,4,8),
     "applying_date" : date(2021,3,5),
     "hour_ID": 15,
     "type" : "BUY",
     "volume": "0.60",
     "price":"30"},
    {"timestamp" : datetime(2021,3,4,8),
     "applying_date" : date(2021,3,5),
     "hour_ID": 16,
     "type" : "BUY",
     "volume": "0.60",
     "price":"40"},
    {"timestamp" : datetime(2021,3,4,8),
     "applying_date" : date(2021,3,5),
     "hour_ID": 17,
     "type" : "SELL",
     "volume": "0.60",
     "price":"50"},
    {"timestamp" : datetime(2021,3,4,8),
     "applying_date" : date(2021,3,5),
     "hour_ID": 17,
     "type" : "SELL",
     "volume": "0.10",
     "price":"80"},
    {"timestamp" : datetime(2021,3,4,8),
     "applying_date" : date(2021,3,6),
     "hour_ID": 17,
     "type" : "SELL",
     "volume": "0.10",
     "price":"80"},
]

@pytest.fixture
def db_worders(db_wkeys):
    key = keys[1]
    _nwritten,_message = db_wkeys.write_orders(key,orders)
    return db_wkeys


def convert_columns(df,types,names):
    for t,n in zip(types,names):
        if t == date:
            df[n] = pd.to_datetime(df[n])
        else:
            df[n] = df[n].astype(t)
    return df

@pytest.fixture
def market_index():
    df = pd.read_csv("./tests/market_data.csv",sep=',')
    return bmrs.convert_columns(df,
                    bmrs.market_index_clean_coltypes,
                    bmrs.market_index_clean_columns)
@pytest.fixture
def imbalance_prices():
    df = pd.read_csv("./tests/imbalance_prices.csv",sep=',')
    return bmrs.convert_columns(df,
                    bmrs.imbalance_prices_selected_coltypes,
                    bmrs.imbalance_prices_selected_columns)

@pytest.fixture
def db_withapidata(db_endpoint,market_index,imbalance_prices):
    db_endpoint.write_market_index(market_index)
    db_endpoint.write_imbalance_prices(imbalance_prices)
    return db_endpoint

@pytest.fixture
def db_withapidata_and_keys(db_endpoint,market_index,imbalance_prices):
    for key in keys:
        db_endpoint.write_key(key)
    db_endpoint.write_market_index(market_index)
    db_endpoint.write_imbalance_prices(imbalance_prices)
    return db_endpoint

@pytest.fixture
def pandas_orders():
    df = pd.read_csv("./tests/orders.csv",sep=',')
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["applying_date"] = pd.to_datetime(df["applying_date"]).dt.date
    return df

@pytest.fixture
def db_complete(db_withapidata_and_keys,pandas_orders):
    key = keys[1]
    _nwritten,_message = db_withapidata_and_keys.write_orders_pandas(key,pandas_orders)
    return db_withapidata_and_keys
