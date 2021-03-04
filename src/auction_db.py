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
                ("key_id", int, ""),
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

    def __init__(self, filename, logger=None, order_deadline = time(9)):
        self.filename = filename
        self.logger = logger
        self.deadline = order_deadline
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
            self._connection.commit()
        self.logger_info(f"Written key")

    def find_key_id(self, key):
        res = self.execute(f'''
        SELECT key_id
        FROM keys
        WHERE encrypted_key == '{self._encrypt(key)}';
        ''').fetchone()
        if res:
            return res[0]

    def check_order_not_late(self,order):
        applying = order["applying_date"]
        deadline = datetime.combine(applying - timedelta(days=1), self.deadline )
        return order["timestamp"] < deadline

    @staticmethod
    def validate_order(order):
        '''
        Check that orders are syntactically valid.
        '''
        return (order["type"].upper() in AuctionDbEndpoint.order_types
                and int(order["hour_ID"]) > 0 and int(order["hour_ID"]) < 25)


    @staticmethod
    def get_field_names(table_name):
        return [t[0] for t in AuctionDbEndpoint.tables[table_name]["fields"]]

    @staticmethod
    def sanify_order(key_id, order):
        if not AuctionDbEndpoint.validate_order(order):
            raise ValueError("Order syntactically invalid.")

        other_fs = {"order_id": None, "key_id": key_id}

        o = (order | other_fs)
        o["type"] = o["type"].upper()

        if type(o["applying_date"]) is str:
            o["applying_date"] = (datetime
                                  .strptime(o["applying_date"], "%Y-%m-%d")
                                  .date())
        assert type(o["applying_date"]) is date
        return o

    @staticmethod
    def to_list(order):
        field_names = AuctionDbEndpoint.get_field_names("orders")
        order = [order[f] for f in field_names]

        return order

    def write_orders(self, key, orders):
        key_id = self.find_key_id(key)

        ords = [self.sanify_order(key_id, o)
                for o in orders]

        ords = [ self.to_list(o) for o in ords
                 if self.check_order_not_late(o)]

        placeholders = ','.join(['?'] * len(self.tables["orders"]["fields"]))
        self.executemany(
            f'''
        INSERT INTO orders VALUES ({placeholders});
        ''', ords)
        self._connection.commit()

        message = ''
        nrejected = len(orders) - len(ords)
        if nrejected:
            message += f"rejected {nrejected} orders because of time limit;"

        self.logger_info(f"Written {len(ords)} orders by id {key_id}")
        return len(ords), message

    def write_orders_pandas(self,key,df):
        fixed_orders = converter.pandas_orders_to_records(df)
        return self.write_orders(key,fixed_orders)

    
    def read_orders(self, key, applying_date, period = None):
        period_selection = f"AND  hour_ID = '{period}'" if period else ''
        q = self.execute(f'''
        SELECT key_id
        FROM keys
        WHERE encrypted_key =  '{self._encrypt(key)}';
        ''').fetchone()
        if not q:
            return None
        else:
            key_id, = q
        field_names = AuctionDbEndpoint.get_field_names("orders")
        selection = ','.join( f'orders.{f}' for f in field_names )

        res = self.execute(f'''
        SELECT {selection},
        ((orders.price <= market_index.price AND orders.type = 'SELL') OR
        (orders.price >= market_index.price AND orders.type = 'BUY') ) as accepted
        FROM orders
        LEFT JOIN market_index
        ON orders.applying_date = market_index.date
        AND orders.hour_ID = market_index.period
        WHERE applying_date = '{date_to_sqlite(applying_date)}'
        {period_selection}
        AND key_id = '{key_id}';
        ''').fetchall()
        field_names.append("accepted")
        res = [ dict(zip(field_names,v)) for v in res ]
        self.logger_info(f"Read {len(res)} orders.")
        return res

    def _write_from_api(self,df_converted,tablename):
        data = df_converted.to_dict("split")["data"]
        placeholders = ','.join(['?'] * len(self.tables[tablename]["fields"]))
        self.executemany(f'''
        REPLACE INTO {tablename}
        VALUES ({placeholders})
        ''',data)
        self._connection.commit()
        self.logger_info(f"Written/replaces {len(data)} rows into {tablename}.")


    def write_imbalance_prices(self, df):
        df_converted = converter.convert_imbalance_prices_columns(df)
        self._write_from_api(df_converted,"imbalance_prices")

    def write_market_index(self, df):
        df_converted = converter.convert_market_index_columns(df)
        self._write_from_api(df_converted,"market_index")

    def _read_api_data(self,start_date,end_date,table_name):
        data = self.execute(f'''
        SELECT *
        FROM {table_name}
        WHERE date >= '{date_to_sqlite(start_date)}'
        AND date < '{date_to_sqlite(end_date)}';
        ''').fetchall()
        header = [ t[0] for t in self.tables[table_name]["fields"]]
        res =  pd.DataFrame(data,columns = header)
        self.logger_info(f"Written/replaces {len(data)} rows into {table_name}.")
        return res

    def read_imbalance_prices(self, start_date, end_date):
        return self._read_api_data(start_date,end_date,"imbalance_prices")

    def read_market_index(self, start_date, end_date):
        return self._read_api_data(start_date,end_date,"market_index")
