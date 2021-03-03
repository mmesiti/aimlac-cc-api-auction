#!/usr/bin/env python3

import query_bmrs as bmrs
import auction_db as db

def extract_db_colnames(table):
    return [ r[0] for r in table["fields"]]


def convert_market_index_columns(df):
    db_fields = extract_db_colnames(db.AuctionDbEndpoint.tables["market_index"])
    pd_cols = bmrs.market_index_clean_columns

    mapper = dict(zip(pd_cols,db_fields))
    return df.rename(mapper=mapper,axis = "columns")


def convert_imbalance_prices_columns(df):
    db_fields = extract_db_colnames(db.AuctionDbEndpoint.tables["imbalance_prices"])
    pd_cols = bmrs.imbalance_prices_selected_columns

    mapper = dict(zip(pd_cols,db_fields))
    return df.rename(mapper=mapper,axis = "columns")
