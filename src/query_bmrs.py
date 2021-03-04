# coding: utf-8
import requests
from datetime import date, datetime, timedelta
import pandas as pd
import os
# information comes from
# https://www.elexon.co.uk/documents/training-guidance/\
# bsc-guidance-notes/bmrs-api-and-data-push-user-guide-2/

apikey = os.getenv("BMRSAPIKEY")
url_base = "https://api.bmreports.com/BMRS"

DATE_FORMAT = "%Y-%m-%d"


def format_date(date_value):
    '''
    to YYYY-MM-DD if not already in this format
    '''
    if type(date_value) == str:
        datetime.strptime(date_value, DATE_FORMAT)
        return date_value
    else:  # date or datetime
        return date_value.strftime(DATE_FORMAT)


def get_market_index(from_date, to_date=None, period='*'):
    REPORT_NAME = "MID"
    VERSION = "V1"

    url = '/'.join([url_base, REPORT_NAME, VERSION])

    if to_date is None:
        to_date = from_date # one day worth of data

    params = {
        "APIKey": apikey,
        "FromSettlementDate": format_date(from_date),
        "ToSettlementDate": format_date(to_date),
        "Period": period,
        "ServiceType": "csv"
    }

    expected_header = ["HDR", "MARKET INDEX DATA"]
    output_fields = [
        "Record Type",
        "Data Provider",
        "Settlement Date",
        "Settlement Period",
        "Price",
        "Volume",
    ]

    r = requests.get(url, params=params)
    lines = r.text.split('\n')
    header = lines[0].split(',')
    assert expected_header == header, f"{header} != {expected_header}"
    data = [l.split(',') for l in lines[1:-1]]
    nrecords = int(lines[-1].split(',')[1])
    assert len(data) == nrecords
    return pd.DataFrame(data, columns=output_fields)


def _get_market_index_clean(from_date, to_date=None, period='*'):
    df = get_market_index(from_date, to_date, period)
    return df.loc[df["Data Provider"] != "N2EXMIDP", :]


def convert_columns(df,types,names):
    for t,n in zip(types,names):
        if t == date:
            df[n] = pd.to_datetime(df[n].astype(str)).dt.date
        else:
            df[n] = df[n].astype(t)
    return df

market_index_clean_columns = [
    "Settlement Date",
    "Settlement Period",
    "Price",
    "Volume"
]

market_index_clean_coltypes = [
    date,
    int,
    float,
    float,
]

def get_market_index_converted(from_date, to_date=None, period='*'):
    df = _get_market_index_clean(from_date, to_date, period)
    return convert_columns(df.drop(["Data Provider", "Record Type"], axis="columns"),
                           market_index_clean_coltypes,
                           market_index_clean_columns)

def get_market_index_clean(from_date, to_date=None, period='*'):
    df_converted = get_market_index_converted(from_date, to_date, period)
    df_converted["Settlement Period"] = (df_converted["Settlement Period"]-1)//2+1
    df_converted = df_converted.groupby(["Settlement Date","Settlement Period"]).mean()
    df_converted["Volume"] *= 2
    return df_converted.reset_index()


imbalance_prices_selected_columns = [
    "SettlementDate",
    "SettlementPeriod",
    "ImbalancePriceAmount",
]

imbalance_prices_selected_coltypes = [
    date,
    int,
    float,
    ]

def get_imbalance_prices(settlement_date, period='*'):
    REPORT_NAME = "B1770"
    VERSION = "V1"

    url = '/'.join([url_base, REPORT_NAME, VERSION])
    params = {
        "APIKey": apikey,
        "SettlementDate": format_date(settlement_date),
        "Period": period,
        "ServiceType": "csv"
    }

    r = requests.get(url, params=params)
    lines = r.text.split('\n')
    header_length = 5
    output_fields = lines[header_length - 1].split(',')
    data = [l.split(',') for l in lines[header_length:-1]]



    df = pd.DataFrame(data, columns=output_fields)
    df = df.loc[df["TimeSeriesID"] == "ELX-EMFIP-IMBP-TS-1",
               imbalance_prices_selected_columns]

    return convert_columns(df,
                           imbalance_prices_selected_coltypes,
                           imbalance_prices_selected_columns)

if __name__ == "__main__":
    r = requests.get(url, params=payload)
    print(r.text)
