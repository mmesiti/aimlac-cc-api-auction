#!/usr/bin/env python3

import query_bmrs as bmrs
import auction_db as db


def extract_db_colnames(table):
    return [r[0] for r in table["fields"]]


def convert_market_index_columns(df):
    db_fields = extract_db_colnames(
        db.AuctionDbEndpoint.tables["market_index"])
    pd_cols = bmrs.market_index_clean_columns

    mapper = dict(zip(pd_cols, db_fields))
    return df.rename(mapper=mapper, axis="columns")


def convert_imbalance_prices_columns(df):
    db_fields = extract_db_colnames(
        db.AuctionDbEndpoint.tables["imbalance_prices"])
    pd_cols = bmrs.imbalance_prices_selected_columns

    mapper = dict(zip(pd_cols, db_fields))
    return df.rename(mapper=mapper, axis="columns")


def pandas_orders_to_records(df):
    orders = df.to_dict('records')

    def fix_ts(order):
        return order | {"timestamp": order["timestamp"].to_pydatetime()}

    def fix_applying_date(order):
        return order | {
            "applying_date": order["applying_date"].strftime("%Y-%m-%d")
        }

    return [fix_applying_date(fix_ts(o)) for o in orders]


def datetime_to_period(timedate):
    raise NotImplementedError


def period_to_datetime(date_, period):
    raise NotImplementedError
