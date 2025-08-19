import argparse, socket
from confluent_kafka.admin import AdminClient, NewTopic

def main():
    ap = argparse.ArgumentParser(description="Create a Kafka topic")
    ap.add_argument("--bootstrap", default="ip_address")
    ap.add_argument("--topic", help="topic name (omit to input interactively)")
    ap.add_argument("--partitions", type=int, default=1)
    ap.add_argument("--replication", type=int, default=1)
    args = ap.parse_args()

    topic = args.topic or input("請輸入要建立的 topic 名稱：").strip()
    if not topic:
        print("未輸入 topic 名稱，結束。"); return

    admin = AdminClient({"bootstrap.servers": args.bootstrap, "client.id": socket.gethostname()})
    md = admin.list_topics(timeout=10)
    if topic in md.topics and not md.topics[topic].error:
        print(f"Topic '{topic}' 已存在。"); return

    fs = admin.create_topics([NewTopic(topic, num_partitions=args.partitions, replication_factor=args.replication)])
    try:
        fs[topic].result()
        print(f"已建立 topic '{topic}' (partitions={args.partitions}, replication={args.replication}).")
    except Exception as e:
        print("建立失敗：", e)

if __name__ == "__main__":
    main()