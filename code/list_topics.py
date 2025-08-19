import argparse, socket
from confluent_kafka.admin import AdminClient

def main():
    ap = argparse.ArgumentParser(description="List Kafka topics")
    ap.add_argument("--bootstrap", default="ip_address")
    args = ap.parse_args()

    admin = AdminClient({"bootstrap.servers": args.bootstrap, "client.id": socket.gethostname()})
    md = admin.list_topics(timeout=10)
    topics = sorted(t for t in md.topics.keys() if not t.startswith("__"))
    print("可用 topics：")
    for t in topics:
        print("-", t)

if __name__ == "__main__":
    main()