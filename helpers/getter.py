import os
import time
import base64
import asyncio
import json
import websockets

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

USE_DEMO = False

if USE_DEMO:
    WS_URL = "wss://demo-api.kalshi.co/trade-api/ws/v2"
    API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "")
    PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "./privkey.txt")
else:
    WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "")
    PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "./privkey.txt")

MARKET_TICKER = "KXNHLTOTAL-26MAR11MTLOTT-4"
OUTFILE = f"{MARKET_TICKER}_ws.jsonl"

def load_private_key(path):
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)

def sign_request(private_key, timestamp_ms, method, path_without_query):
    msg = f"{timestamp_ms}{method.upper()}{path_without_query}".encode("utf-8")
    sig = private_key.sign(
        msg,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("utf-8")

def make_headers(api_key_id, private_key):
    ts = str(int(time.time() * 1000))
    path = "/trade-api/ws/v2"
    sig = sign_request(private_key, ts, "GET", path)
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": sig,
    }

async def main():
    private_key = load_private_key(PRIVATE_KEY_PATH)
    headers = make_headers(API_KEY_ID, private_key)

    async with websockets.connect(
        WS_URL,
        additional_headers=headers,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
    ) as ws:
        print("connected")

        subscription = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta", "trade"],
                "market_tickers": [MARKET_TICKER],
            },
        }

        await ws.send(json.dumps(subscription))
        print(f"subscribed to {MARKET_TICKER}")
        print(f"writing to {OUTFILE}")

        with open(OUTFILE, "a", encoding="utf-8") as f:
            while True:
                msg = await ws.recv()
                print(msg)
                f.write(msg + "\n")
                f.flush()

asyncio.run(main())
