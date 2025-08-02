""" ZMQ Proxy. """

import zmq
import dotenv
import logging
import os
import websockets
import asyncio
import json
from typing import Set

dotenv.load_dotenv()
#logging.basicConfig(level=logging.INFO)

try:
    BINANCE_WS = os.environ["BINANCE_WS"]
    BINANCE_ZMQ = os.environ["BINANCE_ZMQ"]
    HYPERLIQUID_WS = os.environ["HYPERLIQUID_WS"]
    HYPERLIQUID_ZMQ = os.environ["HYPERLIQUID_ZMQ"]
except KeyError as e:
    logging.error(f"Copy the .env_example file, {e}")

binance_clients: Set = set()
hyperliquid_clients: Set = set()

async def binance_handler(websocket):
    """Handle Binance WebSocket connections"""
    binance_clients.add(websocket)
    try:
        async for message in websocket:
            logging.info(f"Binance received: {message}")
    except websockets.exceptions.ConnectionClosed:
        logging.info("Binance WebSocket connection closed")
    finally:
        binance_clients.discard(websocket)

async def hyperliquid_handler(websocket):
    """Handle Hyperliquid WebSocket connections"""
    hyperliquid_clients.add(websocket)
    try:
        async for message in websocket:
            logging.info(f"Hyperliquid received: {message}")
    except websockets.exceptions.ConnectionClosed:
        logging.info("Hyperliquid WebSocket connection closed")
    finally:
        hyperliquid_clients.discard(websocket)

async def broadcast_to_binance_clients(data):
    """Send data to all connected Binance clients"""
    if binance_clients:
        message = json.dumps(data)
        await asyncio.gather(
            *[client.send(message) for client in binance_clients],
            return_exceptions=True
        )

async def broadcast_to_hyperliquid_clients(data):
    """Send data to all connected Hyperliquid clients"""
    if hyperliquid_clients:
        message = json.dumps(data)
        await asyncio.gather(
            *[client.send(message) for client in hyperliquid_clients],
            return_exceptions=True
        )

async def zmq_receiver():
    """Handle ZMQ message receiving in a separate thread"""
    context = zmq.Context()
    binance_socket = context.socket(zmq.SUB)
    binance_socket.bind(BINANCE_ZMQ)
    binance_socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    hyperliquid_socket = context.socket(zmq.SUB)
    hyperliquid_socket.bind(HYPERLIQUID_ZMQ)
    hyperliquid_socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    poller = zmq.Poller()
    poller.register(binance_socket, zmq.POLLIN)
    poller.register(hyperliquid_socket, zmq.POLLIN)
    
    try:
        while True:
            events = poller.poll(timeout=1000)
            
            for socket, event in events:
                if event == zmq.POLLIN:
                    try:
                        data = socket.recv_string()
                        
                        if socket == binance_socket:
                            logging.info(f"Received from Binance ZMQ: {data}")
                            await broadcast_to_binance_clients({"source": "binance", "data": data})
                        elif socket == hyperliquid_socket:
                            logging.info(f"Received from Hyperliquid ZMQ: {data}")
                            await broadcast_to_hyperliquid_clients({"source": "hyperliquid", "data": data})
                            
                    except Exception as e:
                        logging.error(f"Error processing ZMQ message: {e}")
            
            # await asyncio.sleep(0.01)
                        
    except Exception as e:
        logging.error(f"Error in ZMQ receiver: {e}")
    finally:
        binance_socket.close()
        hyperliquid_socket.close()
        context.term()

async def run_proxy():
    binance_server = await websockets.serve(binance_handler, "localhost", 6667)
    hyperliquid_server = await websockets.serve(hyperliquid_handler, "localhost", 6669)
    
    logging.info("WebSocket servers started on localhost:6667 and localhost:6669")
    
    try:
        # Run ZMQ receiver and WebSocket servers concurrently
        await asyncio.gather(
            zmq_receiver(),
            binance_server.wait_closed(),
            hyperliquid_server.wait_closed()
        )
    except KeyboardInterrupt:
        logging.info("Shutting down proxy...")
    finally:
        # Clean up
        binance_server.close()
        hyperliquid_server.close()
        await binance_server.wait_closed()
        await hyperliquid_server.wait_closed()
        logging.info("Proxy shutdown complete")

if __name__ == "__main__":
    asyncio.run(run_proxy())
