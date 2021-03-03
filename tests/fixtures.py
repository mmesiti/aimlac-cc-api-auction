#!/usr/bin/env python3

import auction_db
import query_bmrs as bmrs

import pytest
from datetime import datetime,date
import pandas as pd
import os

@pytest.fixture
def db_endpoint():
    yield auction_db.AuctionDbEndpoint("test.db")
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
