"""Read-only Kafka broker and replica health for the FDM training dashboard."""
import os
from confluent_kafka.admin import AdminClient


def snapshot():
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"], "socket.timeout.ms": 5000})
    metadata = admin.list_topics(timeout=5)
    if metadata.orig_broker_id == -1 and not metadata.brokers:
        raise RuntimeError("Kafka metadata unavailable")
    brokers = {int(bid): {"id": int(bid), "host": broker.host, "port": broker.port, "registered": True, "leader_partitions": 0, "replica_partitions": 0, "in_sync_partitions": 0} for bid, broker in metadata.brokers.items()}
    total = under = unavailable = 0
    affected = []
    for name, topic in metadata.topics.items():
        if not name.startswith('olist.') or topic.error is not None:
            continue
        for pid, p in topic.partitions.items():
            total += 1
            leader = int(p.leader)
            replicas = list(p.replicas)
            isr = list(p.isrs)
            if leader in brokers:
                brokers[leader]['leader_partitions'] += 1
            for bid in replicas:
                if bid in brokers:
                    brokers[bid]['replica_partitions'] += 1
            for bid in isr:
                if bid in brokers:
                    brokers[bid]['in_sync_partitions'] += 1
            if len(isr) < len(replicas) or leader < 0:
                under += 1
                if len(affected) < 15:
                    affected.append({"topic": name, "partition": pid, "leader": leader, "replicas": replicas, "isr": isr})
            if leader < 0:
                unavailable += 1
    # Metadata lists currently registered brokers, not Docker container status.
    # In this known local 3-broker lab, absent IDs are reported as not registered.
    for bid in (1, 2, 3):
        brokers.setdefault(bid, {"id": bid, "host": None, "port": None, "registered": False, "leader_partitions": 0, "replica_partitions": 0, "in_sync_partitions": 0})
    return {"brokers": sorted(brokers.values(), key=lambda b: b['id']), "registered_brokers": sum(b['registered'] for b in brokers.values()), "expected_brokers": 3, "total_partitions": total, "under_replicated_partitions": under, "unavailable_partitions": unavailable, "affected_partitions": affected, "healthy": sum(b['registered'] for b in brokers.values()) == 3 and under == 0 and unavailable == 0}
