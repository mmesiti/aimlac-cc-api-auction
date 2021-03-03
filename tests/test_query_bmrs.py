#!/usr/bin/env python3

import query_bmrs as bmrs
from datetime import date,datetime
import pytest


def test_format_date_goodstr():
    date_string = bmrs.format_date("2021-01-01")
    datetime.strptime(date_string,bmrs.DATE_FORMAT)

def test_format_date_badstr():
    with pytest.raises(ValueError):
        date_string = bmrs.format_date("2021-1")

def test_format_date_datetimevalue():
    timedate = datetime(2021,3,2,12,34,5)
    date_string = bmrs.format_date(timedate)
    datetime.strptime(date_string,bmrs.DATE_FORMAT)


def test_get_market_index():
    df = bmrs.get_market_index("2021-01-01","2021-01-03")
    assert df.shape == (200,6)

def test_get_imbalance_prices():
    df = bmrs.get_imbalance_prices("2021-01-01")
    assert df.shape == (96,3)


@pytest.mark.parametrize("d",["2021-02-14",
                              "2020-01-18",
                              "2020-09-28",
                              "2021-03-02",
                              "2021-12-31"])
def test_N2EXMIDP_is_zero(d):
    def check_date(date):
        df = bmrs.get_market_index("2021-01-01")
        values = df.loc[df["Data Provider"] == "N2EXMIDP","Price"].drop_duplicates()
        assert len(values) == 1
        assert float(values.values[0]) == 0

def test__get_market_index_clean():
    df = bmrs._get_market_index_clean("2021-02-19")
    assert "N2EXMIDP" not in df["Data Provider"].values

def test_get_market_index_clean():
    df = bmrs.get_market_index_clean("2021-02-19")
    assert "Data Provider" not in df
    assert "Record Type" not in df

def test_market_index_clean_columns():
    df = bmrs.get_market_index_clean("2021-02-19")
    assert set(df.columns) == set(bmrs.market_index_clean_columns)

def test_imbalance_prices_selected_columns():
    df = bmrs.get_imbalance_prices("2021-02-19")
    assert set(df.columns) == set(bmrs.imbalance_prices_selected_columns)
