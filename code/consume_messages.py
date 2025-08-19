import argparse, socket
from datetime import datetime
from confluent_kafka import Consumer

def main():
    ap = argparse.ArgumentParser(description="Consume Kafka messages and print time + value")
    ap.add_argument("--bootstrap", default="ip_address")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--group", default="g-home")
    ap.add_argument("--from-beginning", action="store_true")
    args = ap.parse_args()

    conf = {
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "enable.auto.commit": True,
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "client.id": socket.gethostname(),
    }
    c = Consumer(conf)
    c.subscribe([args.topic])
    print(f"開始接收：topic='{args.topic}', group='{args.group}', from_beginning={args.from_beginning} … Ctrl+C 結束")
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None: continue
            if msg.error():
                print("[error]", msg.error()); continue
            _, ts = msg.timestamp()
            tstr = datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d %H:%M:%S") if ts and ts > 0 else "-"
            key = msg.key().decode() if msg.key() else None
            val = msg.value().decode(errors="replace") if msg.value() else ""
            print(f"[{tstr}] {args.topic}[{msg.partition()}]@{msg.offset()} key={key} value={val}")
    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == "__main__":
    main()