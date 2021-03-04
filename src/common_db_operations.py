#!/usr/bin/env python3
import sqlite3
from datetime import datetime, date


def datetime_to_sqlite(td):
    return datetime.isoformat(td).replace('T', ' ')


def date_to_sqlite(td):
    return date.isoformat(td)


def setup_db(filename, schema):
    '''
    schema is a dictionary "table name": <field specification>,
    where <field specification> is a list of pairs ("field name",<python type>)
    '''
    conn = sqlite3.connect(filename,
                           detect_types=sqlite3.PARSE_DECLTYPES
                           | sqlite3.PARSE_COLNAMES)
    c = conn.cursor()

    def convert_fields(table_fields):
        sqlite_types = {
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            datetime: "TIMESTAMP",
            date: "DATE",
        }
        return [(k, sqlite_types[v], mod) for k, v, mod in table_fields]

    def schema_to_tuples(tables):
        return [(k, convert_fields(v["fields"]), v["constraints"])
                for k, v in tables.items()]

    for table in schema_to_tuples(schema):
        k, vs, constrs = table
        fieldspec = ','.join([f"{name} {t} {mod}" for name, t, mod in vs])
        constraints = constrs.strip()
        constraints = f", {constraints}" if constraints else ''

        c.execute(f"CREATE TABLE {k} ({fieldspec}{constraints});")

    return c, conn
