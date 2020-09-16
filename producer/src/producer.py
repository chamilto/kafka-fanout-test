from argparse import ArgumentParser
import json
import logging
import sys
import random

from kafka import KafkaProducer

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

_PRODUCER_CLIENT_ID = "kafka-fanout-test-simple-producer"
_EXAMPLE_EVENT_TYPES = ["on_event1", "on_event2"]


def _get_arg_parser():
    ap = ArgumentParser()
    ap.add_argument("-t", "--topic", help="topic to which messages will be published", dest="topic", required=True)
    ap.add_argument(
        "-n",
        "--num-messages",
        type=int,
        help="number of messages to produce",
        dest="num_messages",
        required=False,
        default=100,
    )
    ap.add_argument(
        "-p", "--partition", help="partition to which messages will be published", dest="partition", required=False
    )
    ap.add_argument(
        "-b",
        "--bootstrap-servers",
        help="host[:port] string/list of strings that the producer should contact to bootstrap initial cluster metadata. defaults to localhost:9092",
        dest="bootstrap_servers",
        required=True,
    )

    return ap


def _serializer(val):
    return json.dumps(val).encode("utf-8")


def _get_producer(bootstrap_servers=None):
    producer_kwargs = dict(client_id=_PRODUCER_CLIENT_ID, value_serializer=_serializer)

    if bootstrap_servers is not None:
        producer_kwargs["bootstrap_servers"] = bootstrap_servers

    return KafkaProducer(**producer_kwargs)


def _on_success(record_metadata):
    logger.info(
        json.dumps(
            {"topic": record_metadata.topic, "partition": record_metadata.partition, "offset": record_metadata.offset}
        )
    )


def _on_error(exc):
    logger.error("error sending message", exc_info=exc)


def _send_message(producer, topic, content):
    producer.send(topic, content).add_callback(_on_success).add_errback(_on_error)


def _produce_messages(producer, topic, num_messages=None):
    if num_messages is None:
        num_messages = 100

    logger.info("sending messages...")

    for i in range(num_messages):
        _send_message(
            producer,
            topic,
            {"message": "hello, world", "message_num": i, "event_type": random.choice(_EXAMPLE_EVENT_TYPES)},
        )

    producer.flush()

    logger.info(f"published {num_messages} messages")


def main():
    args = _get_arg_parser().parse_args()
    producer = _get_producer(args.bootstrap_servers)
    _produce_messages(producer, args.topic, num_messages=args.num_messages)


if __name__ == "__main__":
    main()
