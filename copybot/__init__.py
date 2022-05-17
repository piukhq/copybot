import json
import logging
from datetime import datetime
from random import randint

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from pythonjsonlogger import jsonlogger
from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from settings import settings

logger = logging.getLogger()
logHandler = logging.StreamHandler()
logFmt = jsonlogger.JsonFormatter(timestamp=True)
logHandler.setFormatter(logFmt)
logger.addHandler(logHandler)

engine = create_engine(settings.postgres_host.format(settings.postgres_db))
Session = sessionmaker(bind=engine)
Base = declarative_base()


class Events(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True)
    event_date_time = Column(DateTime)
    event_type = Column(String)
    json = Column(JSON)


Base.metadata.create_all(engine)


def dead_letter(msg: dict) -> None:
    with pika.BlockingConnection(pika.URLParameters(settings.amqp_url)) as connection:
        channel = connection.channel()
        channel.queue_declare(queue="clickhouse_deadletter", durable=True)
        channel.basic_publish(exchange="", routing_key="clickhouse_deadletter", body=json.dumps(msg))


def process_message(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, message: bytes) -> None:
    """
    Process Message from RabbitMQ Queue and Push to Postgres
    """
    try:
        raw_msg = json.loads(message.decode())
        event_date_time = raw_msg.pop("event_date_time")
        event_type = raw_msg.pop("event_type")
        event = {
            "event_date_time": event_date_time,
            "event_type": event_type,
            "json": raw_msg,
        }
    except KeyError:
        raise KeyError("Message does not contain the required JSON fields")

    retries = 3
    while True:
        try:
            if settings.debug:
                logging.warning(msg="Processing Event", extra={"event": event})
            else:
                logging.warning(msg="Processing Event")
            insert = Events(**event)

            with Session() as session:
                session.add(insert)
                session.commit()
            ch.basic_ack(delivery_tag=method.delivery_tag)
            break
        except Exception as ex:
            retries -= 1
            if retries < 1:
                logging.warning(msg="Event processing failed, sending to Dead Letter Queue", exc_info=ex)
                dead_letter(message.decode())
                ch.basic_ack(delivery_tag=method.delivery_tag)
                break
            else:
                logging.warning(msg="Event processing failed, retrying", exc_info=ex)
                continue


def rabbitmq_message_put(count: int, queue: str) -> None:
    """
    Puts n messages onto RabbitMQ queue for later consumption
    """
    with pika.BlockingConnection(pika.URLParameters(settings.amqp_url)) as connection:
        channel = connection.channel()
        channel.queue_declare(queue=queue)
        logging.warning(
            msg="Pushing Messages to Message Bus",
            extra={"count": count, "queue": queue},
        )
        for _ in range(count):
            msg_payload = {
                "event_type": "event.user.created.api",
                "origin": "channel",
                "channel": "bink",
                "event_date_time": f"{datetime.now().strftime(settings.datetime_format)}",
                "external_user_ref": str(randint(100000000, 999999999)),
                "internal_user_ref": randint(1, 999),
                "email": "cpressland@bink.com",
            }
            logging.warning(msg="Pushing Message", extra={"payload": msg_payload, "queue": queue})
            channel.basic_publish(
                exchange="",
                routing_key=queue,
                body=json.dumps(msg_payload),
            )


def rabbitmq_message_get(queue: str) -> None:
    """
    Gets Messages from RabbitMQ Forever
    """
    with pika.BlockingConnection(pika.URLParameters(settings.amqp_url)) as connection:
        channel = connection.channel()
        channel.basic_consume(
            queue=queue,
            on_message_callback=process_message,
            auto_ack=False,
        )
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
