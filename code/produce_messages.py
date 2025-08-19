import argparse, sys, socket
from confluent_kafka import Producer

def send(p, topic, key, value):
    def cb(err, msg):
        if err:
            print("送出失敗：", err)
        else:
            print(f"sent {msg.topic()}[{msg.partition()}]@{msg.offset()} key={msg.key()} value={msg.value()!r}")
    p.produce(topic=topic, key=key, value=value, callback=cb)
    p.poll(0)

def produce_manual(p, topic):
    print("請逐行輸入訊息，Ctrl+C 或空行離開：")
    try:
        while True:
            line = input("> ").rstrip("\n")
            if line == "": break
            send(p, topic, None, line.encode())
    except (KeyboardInterrupt, EOFError):
        pass

def produce_excel(p, topic, path, sheet):
    import pandas as pd
    df = pd.read_excel(path, sheet_name=sheet) if sheet else pd.read_excel(path)
    cols = [c.lower() for c in df.columns]
    has_key = "key" in cols
    if "value" not in cols:
        first_col = df.columns[0]
        df = df.rename(columns={first_col: "value"})
    else:
        df = df.rename(columns={c: c.lower() for c in df.columns})
    count = 0
    for _, row in df.iterrows():
        val = row.get("value")
        if pd.isna(val): continue
        key = row.get("key") if has_key else None
        send(p, topic, None if key is None or (isinstance(key, float) and pd.isna(key)) else str(key).encode(), str(val).encode())
        count += 1
    p.flush(10)
    print(f"已從 Excel 送出 {count} 筆。")

def main():
    ap = argparse.ArgumentParser(description="Produce Kafka messages (manual or Excel)")
    ap.add_argument("--bootstrap", default="ip_address")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--message", help="直接送出一筆訊息文字")
    ap.add_argument("--excel", help="讀取 Excel 檔路徑（.xlsx）")
    ap.add_argument("--sheet", help="Excel 的工作表名稱（省略則使用第一個）")
    args = ap.parse_args()

    p = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": socket.gethostname(),
        "acks": "all",
        "linger.ms": 5,
        "retries": 3,
    })

    if args.excel:
        produce_excel(p, args.topic, args.excel, args.sheet)
    elif args.message is not None:
        send(p, args.topic, None, args.message.encode()); p.flush(10)
    else:
        produce_manual(p, args.topic); p.flush(10)

if __name__ == "__main__":
    main()