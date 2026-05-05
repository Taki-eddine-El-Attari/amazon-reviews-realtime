import logging
import sys
import importlib.util
from pathlib import Path


def _bootstrap_kafka_vendor_six_moves():
    module_name = "kafka.vendor.six"
    if "kafka.vendor.six.moves" in sys.modules:
        return

    six_path = Path(__file__).resolve().parents[1] / ".venv" / "Lib" / "site-packages" / "kafka" / "vendor" / "six.py"
    spec = importlib.util.spec_from_file_location(module_name, six_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    sys.modules["kafka.vendor.six.moves"] = module.moves


_bootstrap_kafka_vendor_six_moves()

from kafka.admin import KafkaAdminClient, NewTopic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TopicCreator")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "amazon-reviews"
NUM_PARTITIONS          = 3    # 3 partitions comme montré dans le PDF
REPLICATION_FACTOR      = 1

def create_topic():
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="topic-creator"
    )

    topic = NewTopic(
        name=KAFKA_TOPIC,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR
    )

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        logger.info(f"Topic '{KAFKA_TOPIC}' créé avec {NUM_PARTITIONS} partitions")
    except Exception as e:
        logger.warning(f"⚠Topic existe déjà ou erreur : {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    create_topic()