#!/usr/bin/env python3
from auction_db import AuctionDbEndpoint
from loggers import get_logger, set_global_handler
import argparse


def make_parser():
    parser = argparse.ArgumentParser(description='''
    Start the
    ''')

    parser.add_argument('--dbfile',
                        type=str,
                        required=True,
                        help="File to use for the sqlite database.")

    parser.add_argument('--key',
                        required=True,
                        type=str,
                        help="Key to encrypt in the database.")

    parser.add_argument('--logfile',
                        required=True,
                        type=str,
                        help="File where to store logs.")

    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    database_file = args.dbfile
    key = args.key
    logfile = args.logfile

    set_global_handler(logfile)
    logger = get_logger("MAIN")

    bid_db = AuctionDbEndpoint(database_file, logger=logger)

    bid_db.write_key(key)
