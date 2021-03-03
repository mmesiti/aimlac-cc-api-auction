#!/usr/bin/env python3
from common_db_operations import setup_db, date_to_sqlite

from datetime import date, datetime, time, timedelta
import os
import sqlite3
import pandas as pd
import hashlib
import converter

class AuctionDbEndpoint:
    tables = {
        "imbalance_prices": {
            "fields": [
                ("date", date, ""),
                ("period", int, ""),
                ("price", float, ""),
            ],
            "constraints": 'UNIQUE (date,period)'},
        "market_index": {
            "fields": [("date", date, ""),
                       ("period", int, ""),
                       ("price", float, ""),
                       ("volume", float, "")],
            "constraints": 'UNIQUE (date,period)'},
        "orders": {
            "fields": [
                ("order_id", int, "PRIMARY KEY AUTOINCREMENT"),
                ("key_id", str, ""),
                ("timestamp", datetime, ""),
                ("applying_date", date, ""),
                ("hour_ID", int, ""),  # 1-24
                ("type", str, ""),
                ("volume", float, ""),
                ("price", float, "")
            ],
            "constraints": ''},
        "keys": {
            "fields": [("key_id", int, "PRIMARY KEY AUTOINCREMENT"),
                       ("encrypted_key", str, "")],
            "constraints": ''}
    }
    order_types = ["BUY", "SELL"]

    def logger_info(self, *args, **kwargs):
        if self.logger:
            self.logger.info(*args, **kwargs)

    def logger_warning(self, *args, **kwargs):
        if self.logger:
            self.logger.warning(*args, **kwargs)

    def logger_debug(self, *args, **kwargs):
        if self.logger:
            self.logger.debug(*args, **kwargs)

    def __init__(self, filename, logger=None):
        self.logger = None
        self.filename = filename
        if not os.path.exists(filename):
            self._cursor, self._connection = setup_db(filename,
                                                      AuctionDbEndpoint.tables)
        else:
            self.logger_info("Connecting to existing database")
            self._connection = sqlite3.connect(
                self.filename, detect_types=sqlite3.PARSE_DECLTYPES)
            self._cursor = self._connection.cursor()

    def executemany(self, *args, **kwargs):
        return self._cursor.executemany(*args, **kwargs)

    def execute(self, *args, **kwargs):
        return self._cursor.execute(*args, **kwargs)

    def _encrypt(self, key):
        return hashlib.sha256(key.encode()).hexdigest()

    def write_key(self, key):
        if not self.find_key_id(key):
            self.execute(f'''
            INSERT INTO keys VALUES (NULL,'{self._encrypt(key)}');
            ''')

    def find_key_id(self, key):
        res = self.execute(f'''
        SELECT key_id
        FROM keys
        WHERE encrypted_key == '{self._encrypt(key)}';
        ''').fetchone()
        if res:
            return res[0]

    @staticmethod
    def check_order_not_late(order):
        applying = order["applying_date"]
        deadline = datetime.combine(applying - timedelta(days=1), time(9))
        return order["timestamp"] < deadline

    @staticmethod
    def validate_order(order):
        '''
        Check that orders are syntactically valid.
        '''
        return (order["type"].upper() in AuctionDbEndpoint.order_types
                and int(order["hour_ID"]) > 0 and int(order["hour_ID"]) < 25)

    @staticmethod
    def process_order(key_id, order):
        if not AuctionDbEndpoint.validate_order(order):
            raise ValueError("Order syntactically invalid.")

        fields = {"order_id": None, "key_id": key_id}

        o = (order | fields)
        o["type"] = o["type"].upper()

        field_names = [t[0] for t in AuctionDbEndpoint.tables["orders"]["fields"]]
        o = [o[f] for f in field_names]

        return o

    def write_orders(self, key, orders):
        key_id = self.find_key_id(key)

        ords = [
            self.process_order(key_id, o) for o in orders
            if self.check_order_not_late(o)
        ]

        placeholders = ','.join(['?'] * len(self.tables["orders"]["fields"]))
        self.executemany(
            f'''
        INSERT INTO orders VALUES ({placeholders});
        ''', ords)

        message = ''
        nrejected = len(orders) - len(ords)
        if nrejected:
            message += f"rejected {nrejected} orders because of time limit;"

        return len(ords), message

    def read_orders_slot(self, key, applying_date, period):
        q = self.execute(f'''
        SELECT key_id
        FROM keys
        WHERE encrypted_key =  '{self._encrypt(key)}';
        ''').fetchone()
        if not q:
            return None
        else:
            key_id, = q

        return self.execute(f'''
        SELECT *
        FROM orders
        WHERE applying_date = '{date_to_sqlite(applying_date)}'
        AND  hour_ID = '{period}'
        AND key_id = '{key_id}';
        ''').fetchall()

    def _write_from_api(self,df_converted,tablename):
        data = df_converted.to_dict("split")["data"]
        placeholders = ','.join(['?'] * len(self.tables[tablename]["fields"]))
        self.executemany(f'''
        REPLACE INTO {tablename}
        VALUES ({placeholders})
        ''',data)


    def write_imbalance_prices(self, df):
        df_converted = converter.convert_imbalance_prices_columns(df)
        self._write_from_api(df_converted,"imbalance_prices")

    def write_market_index(self, df):
        df_converted = converter.convert_market_index_columns(df)
        self._write_from_api(df_converted,"market_index")


    def read_imbalance_prices(self, start, end):
        pass

    def read_market_index(self, start, end):
        pass
