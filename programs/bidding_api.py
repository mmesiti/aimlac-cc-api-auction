#!/usr/bin/env python3

from loggers import get_logger, set_global_handler
from bid_api import get_app

import argparse
from flask_socketio import SocketIO
import eventlet

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

    parser.add_argument('--endpointname',
                        required=True,
                        type=str,
                        help="The endpoint will be '/endpointname'.")

    parser.add_argument('--port',
                        required=True,
                        type=str,
                        help="The port to run the webserver on.")

    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    database_file = args.dbfile
    logfile = args.logfile
    endpoint_name = args.endpointname
    port = args.port

    set_global_handler(logfile)
    logger = get_logger("MAIN")

    app = get_app(database_file, logger, endpoint_name)

    eventlet.monkey_patch()
    socketio = SocketIO()

    socketio.init_app(app,port=port)
    socketio.run(app,host="0.0.0.0" )
