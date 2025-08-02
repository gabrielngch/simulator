""" Exchange data ingestor. Currently supports Hyperliquid and Binance data. """

import csv
import heapq
import json
import logging
import os
import dotenv
import zmq

from datetime import datetime, UTC

dotenv.load_dotenv()

try:
    BINANCE_ZMQ = os.environ["BINANCE_ZMQ"]
    HYPERLIQUID_ZMQ = os.environ["HYPERLIQUID_ZMQ"]
except KeyError as e:
    logging.error(f"Copy the .env_example file, {e}")

"""
TODO notes:
1. add latencies
2. engine timestamps, figure out how to comm with the system. ask tejas
"""

def queue_data(ticker: str, date: datetime):

    """
    Queues data through a websocket. This will mimic the Binance/Hyperliquid websockets.
    In the parent directory, you will require a data folder with exchange data. See README 
    to understand how the data should be formated/named.

    Parameters:
        ticker: ticker for which data is to be ingested, in the form sol, btc, eth for example.
        date: queues one day of data at a time

    Returns:
        None.
    """
    # zmq connections to proxy sockets
    context = zmq.Context()
    binance_socket = context.socket(zmq.PUB)
    binance_socket.connect(BINANCE_ZMQ)
    hyperliquid_socket = context.socket(zmq.PUB)
    hyperliquid_socket.connect(HYPERLIQUID_ZMQ)
    
    # formatting
    ticker = ticker.lower()
    date = date.strftime("%Y-%m-%d")
    binance_trades = open(f"data/binanceusdm_{ticker}_aggTrade_{date}.csv", "r")
    #binance_l2 = open(f"data/binanceusdm_{ticker}_depth20_{date}.csv", "r")
    binance_l1 = open(f"data/binanceusdm_{ticker}_bookticker_{date}.csv", "r")
    #hyperliquid_trades = open(f"data/hyperliquid_{ticker}_trades_{date}", "r")
    #hyperliquid_l2 = open(f"data/hyperliquid_{ticker}_l2Book_{date}", "r")

    heap = []
    readers = {}
    files = {
        "binance_trades": binance_trades,
        "binance_l1": binance_l1,
    }


    for k, f in files.items():
        reader = csv.reader(f)
        readers[k] = reader
        try:
            row = next(reader)
            timestamp = int(row[3]) # simulate latency here
            heapq.heappush(heap, (timestamp, k, row))

        except StopIteration:
            logging.error(f"{k} file at {date} for ticker {ticker} is empty")
            return

    while heap:
        timestamp, key, row = heapq.heappop(heap)
        match key:
            # send data through websocket here
            case "binance_trades":
                binance_trades_callback(timestamp, ticker, row, binance_socket)
            case "binance_l1":
                binance_l1_callback(timestamp, ticker, row, binance_socket)
            case _:
                pass
        
        try:
            next_row = next(readers[k])
            next_timestamp = int(next_row[3])
            heapq.heappush(heap, (next_timestamp, k, next_row))
        except StopIteration:
            continue # exhausted file

def binance_trades_callback(timestamp, ticker, row, socket):
    message = {
        "e": "aggTrade",  # Event type
        "E": timestamp,   # Event time
        "s": f"{ticker.upper()}USDT",  # Symbol
        "a": int(row[4]),  # Aggregate trade ID
        "p": row[5],       # Price
        "q": row[6],       # Quantity
        "f": int(row[7]),  # First trade ID
        "l": int(row[8]),  # Last trade ID
        "T": int(row[3]),  # Trade time
        "m": row[9] == "True",  # Is buyer maker
        "M": True          # Ignore
    }
    socket.send_string(json.dumps(message))

def binance_l1_callback(timestamp, ticker, row, socket):
    message = {
        "e": "bookTicker",  # Event type
        "E": timestamp,     # Event time
        "s": f"{ticker.upper()}USDT",  # Symbol
        "b": row[4],        # Best bid price
        "B": row[5],        # Best bid quantity
        "a": row[6],        # Best ask price
        "A": row[7],        # Best ask quantity
        "T": timestamp,     # Transaction time
        "u": timestamp      # Update ID
    }
    socket.send_string(json.dumps(message))

def binance_l2_callback(data, socket):
    pass

def hyperliquid_trades_callback(data, socket):
    pass

def hyperliquid_l2_callback(data, socket):
    pass

if __name__ == "__main__":
    queue_data("sol", datetime(2025,6,18,tzinfo=UTC))


