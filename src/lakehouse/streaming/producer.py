"""Phase 6 - Real-time lane, part 1: Binance live trades (websocket) -> Kafka.

    python -m lakehouse.streaming.producer                    # runs until Ctrl+C
    python -m lakehouse.streaming.producer --seconds 300      # 5-minute demo

Runs in your Windows venv and talks to Kafka on localhost:9092.

Kafka concepts used here (all interview staples):
  * Topic `binance.trades` with 3 partitions; message KEY = symbol. Kafka guarantees order
    only within a partition, and the key decides the partition - so every BTC trade lands
    in the same partition, in order.
  * Idempotent producer (enable.idempotence + acks=all): if a send is retried after a
    network blip, the broker de-duplicates it. No double-counted trades.
  * Batching (linger.ms) + lz4 compression: fewer, bigger requests - higher throughput.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import threading
import time
from datetime import UTC, datetime

from lakehouse.config import load_settings

TOPIC = "binance.trades"
# data-stream.binance.vision is Binance's market-data-only endpoint (no account needed).
WS_BASE = os.getenv("BINANCE_WS_URL", "wss://data-stream.binance.vision")


def to_record(raw: str) -> tuple[str, dict] | None:
    """Binance combined-stream message -> (kafka key, kafka value). None if not a trade."""
    msg = json.loads(raw)
    d = msg.get("data", msg)
    if d.get("e") != "trade":
        return None
    return d["s"], {
        "symbol": d["s"],
        "trade_id": d["t"],
        "price": d["p"],  # keep as string: exact decimal, Spark casts it
        "qty": d["q"],
        "trade_time_ms": d["T"],
        "event_time_ms": d["E"],
        "is_buyer_maker": d["m"],  # True => the aggressive side was a SELL
        "ingested_at_ms": int(time.time() * 1000),
    }


def ensure_topic(bootstrap: str, partitions: int = 3) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    if TOPIC in admin.list_topics(timeout=10).topics:
        return
    fut = admin.create_topics([NewTopic(TOPIC, num_partitions=partitions, replication_factor=1)])
    fut[TOPIC].result()
    print(f"created topic {TOPIC} ({partitions} partitions)")


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.received = self.delivered = self.failed = 0
        self.started = time.time()

    def line(self) -> str:
        secs = max(time.time() - self.started, 1e-9)
        return (
            f"received={self.received:,} delivered={self.delivered:,} failed={self.failed} "
            f"avg={self.delivered / secs:,.1f} msg/s"
        )


def main(argv: list[str] | None = None) -> int:
    import websocket
    from confluent_kafka import Producer

    settings = load_settings()
    ap = argparse.ArgumentParser(description="Stream Binance trades into Kafka")
    ap.add_argument("--symbols", default=",".join(settings.streaming_symbols))
    ap.add_argument("--seconds", type=int, default=0, help="stop after N seconds (0 = forever)")
    args = ap.parse_args(argv)

    bootstrap = settings.kafka_bootstrap_servers
    ensure_topic(bootstrap)
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "compression.type": "lz4",
            "client.id": "binance-trades-producer",
        }
    )
    stats = Stats()
    stop = threading.Event()

    def on_delivery(err, _msg):
        with stats.lock:
            if err:
                stats.failed += 1
            else:
                stats.delivered += 1

    def on_message(_ws, raw):
        rec = to_record(raw)
        if rec is None:
            return
        key, value = rec
        stats.received += 1
        while True:
            try:
                producer.produce(TOPIC, key=key, value=json.dumps(value), on_delivery=on_delivery)
                break
            except BufferError:  # local queue full: let it drain, then retry
                producer.poll(0.5)
        producer.poll(0)  # serve delivery callbacks

    streams = "/".join(f"{s.lower()}@trade" for s in args.symbols.split(","))
    url = f"{WS_BASE}/stream?streams={streams}"
    ws = websocket.WebSocketApp(
        url,
        on_message=on_message,
        on_open=lambda _ws: print(f"connected: {url}"),
        on_error=lambda _ws, e: print(f"websocket error: {e}"),
        on_close=lambda *_: print("websocket closed"),
    )

    def shutdown(*_):
        stop.set()
        ws.close()

    signal.signal(signal.SIGINT, shutdown)
    if args.seconds:
        threading.Timer(args.seconds, shutdown).start()

    def report():
        while not stop.wait(10):
            print(f"[{datetime.now(UTC):%H:%M:%S} UTC] {stats.line()}")

    threading.Thread(target=report, daemon=True).start()
    # reconnect=5: if Binance drops the connection (they do every 24 h), retry after 5 s.
    while not stop.is_set():
        ws.run_forever(ping_interval=20, ping_timeout=10, reconnect=5)
        if not stop.is_set():
            time.sleep(5)

    remaining = producer.flush(15)  # deliver everything still buffered before exiting
    print(f"\nfinal: {stats.line()} | undelivered after flush: {remaining}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
