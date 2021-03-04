#!/usr/bin/env python3

import query_bmrs as bmrs
from datetime import datetime
import pytest


def test_format_date_goodstr():
    date_string = bmrs.format_date("2021-01-01")
    datetime.strptime(date_string, bmrs.DATE_FORMAT)


def test_format_date_badstr():
    with pytest.raises(ValueError):
        date_string = bmrs.format_date("2021-1")

        assert date_string is not None


def test_format_date_datetimevalue():
    timedate = datetime(2021, 3, 2, 12, 34, 5)
    date_string = bmrs.format_date(timedate)
    datetime.strptime(date_string, bmrs.DATE_FORMAT)


def test_get_market_index():
    df = bmrs.get_market_index("2021-01-01", "2021-01-03")
    assert df.shape == (200, 6)


def test_get_imbalance_prices():
    df = bmrs.get_imbalance_prices("2021-01-01")
    assert df.shape == (48, 3)


@pytest.mark.parametrize(
    "d",
    ["2021-02-14", "2020-01-18", "2020-09-28", "2020-12-31", "2021-03-02"])
def test_N2EXMIDP_is_zero(d):
    df = bmrs.get_market_index(d)
    values = df.loc[df["Data Provider"] == "N2EXMIDP",
                    "Price"].drop_duplicates()
    assert len(values) == 1
    assert float(values.values[0]) == 0


def test__get_market_index_clean():
    df = bmrs._get_market_index_clean("2021-02-19")
    assert "N2EXMIDP" not in df["Data Provider"].values


def test_get_market_index_clean():
    df = bmrs.get_market_index_converted("2021-02-19")
    assert "Data Provider" not in df
    assert "Record Type" not in df


def test_market_index_clean_columns():
    df = bmrs.get_market_index_converted("2021-02-19")
    assert set(df.columns) == set(bmrs.market_index_clean_columns)


def test_market_index_1_24():
    df = bmrs.get_market_index_clean("2021-02-19")
    assert not any(df["Settlement Period"] > 24)
    assert not any(df["Settlement Period"] < 1)


def test_avg_and_sum():
    df_converted = bmrs.get_market_index_converted("2021-02-19")
    df_clean = bmrs.get_market_index_clean("2021-02-19")
    assert df_converted["Price"].iloc[:2].mean() == df_clean["Price"].iloc[0]
    assert df_converted["Volume"].iloc[:2].sum() == df_clean["Volume"].iloc[0]


def test_imbalance_prices_selected_columns():
    df = bmrs.get_imbalance_prices("2021-02-19")
    assert set(df.columns) == set(bmrs.imbalance_prices_selected_columns)
