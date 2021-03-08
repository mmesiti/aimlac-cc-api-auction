#!/usr/bin/env python3

from loggers import get_logger, set_global_handler
import query_bmrs as bmrs
from auction_db import AuctionDbEndpoint

import argparse
from datetime import date


def make_parser():
    parser = argparse.ArgumentParser(description='''
    Start the
    ''')

    parser.add_argument('--dbfile',
                        type=str,
                        required=True,
                        help="File to use for the sqlite database.")

    parser.add_argument('--logfile',
                        required=True,
                        type=str,
                        help="File where to store logs.")

    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    database_file = args.dbfile
    logfile = args.logfile

    set_global_handler(logfile)
    logger = get_logger("MAIN")

    bid_db = AuctionDbEndpoint(database_file, logger=logger)

    # get data from yesterday
    yesterday = date.today()
    market_index_df = bmrs.get_market_index_converted(yesterday.isoformat())
    imbalance_prices_df = bmrs.get_imbalance_prices(yesterday.isoformat(0))

    bid_db.write_market_index(market_index_df)
    bid_db.write_imbalance_price(imbalance_prices_df)
