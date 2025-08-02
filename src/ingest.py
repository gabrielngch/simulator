""" Exchange data ingestor. Currently supports Hyperliquid and Binance data. """

import csv
import heapq

from datetime import datetime, UTC

BINANCE_FORMAT = [
    "binanceusdm_{ticker}_aggTrade_{date}",
    "binanceusdm_{ticker}_depth20_{date}",
    "binanceusdm_{ticker}_bookticker_{date}",
]
HYPERLIQUID_FORMAT = [
    "hyperliquid_{ticker}_trades_{date}",
    "hyperliquid_{ticker}_l2Book_{date}",
]

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
            timestamp = int(row[3])
            heapq.heappush(heap, (timestamp, k, row))

        except StopIteration:
            continue #empty file

    while heap:
        timestamp, key, row = heapq.heappop(heap)
        match key:
            case "binance_trades":
                print(row)
            case "binance_l1":
                print(row)
            case _:
                pass
        
        try:
            next_row = next(readers[k])
            next_timestamp = int(next_row[3])
            heapq.heappush(heap, (next_timestamp, k, next_row))
        except StopIteration:
            continue #exhausted file

if __name__ == "__main__":
    queue_data("sol", datetime(2025,6,18,tzinfo=UTC))

