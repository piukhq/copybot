import json
import logging
from datetime import datetime, timedelta
from importlib.metadata import version
from random import choice, randint
from time import sleep

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from prometheus_client import Counter
from pythonjsonlogger import jsonlogger
from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from copybot.settings import settings

__version__ = version("copybot")

logger = logging.getLogger()
logHandler = logging.StreamHandler()
logFmt = jsonlogger.JsonFormatter(timestamp=True)
logHandler.setFormatter(logFmt)
logger.addHandler(logHandler)

engine = create_engine(settings.postgres_host.format(settings.postgres_db), connect_args=settings.postgres_connect_args)
Session = sessionmaker(bind=engine)
Base = declarative_base()

total_events_processed = Counter("events_processed", "Count of messages processed")


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


def pg_cleanup(days: int, splay: int) -> None:
    """
    Delete old database records
    """
    rand_splay = randint(0, splay)
    logging.warning(msg="Beginning Cleanup Operation", extra={"days_to_keep": days, "random_delay": rand_splay})
    sleep(rand_splay)
    delta = datetime.now() - timedelta(days=days)
    with Session() as session:
        query = session.query(Events).filter(Events.event_date_time <= delta)
        logging.warning(msg="Records Deleted", extra={"rows": query.count()})
        query.delete()
        session.commit()


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
            total_events_processed.inc()
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
            event_date_time = datetime.now() - timedelta(
                days=randint(0, 1000),
                hours=randint(0, 24),
                minutes=randint(0, 60),
                seconds=randint(0, 60),
            )
            event_types = [
                "user.session.start",
                "lc.auth.failed",
                "lc.addandauth.success",
                "lc.register.request",
                "lc.join.request",
                "user.created",
                "lc.auth.success",
                "lc.register.success",
                "lc.join.failed",
                "user.deleted",
                "lc.auth.request",
                "transaction.exported",
                "lc.join.success",
                "lc.statuschange",
                "lc.addandauth.request",
                "payment.account.status.change",
                "lc.addandauth.failed",
                "payment.account.added",
                "payment.account.removed",
                "lc.register.failed",
                "lc.removed",
            ]
            msg_payload = {
                "event_type": choice(event_types),
                "origin": "channel",
                "channel": "bink",
                "event_date_time": f"{event_date_time.strftime(settings.datetime_format)}",
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
    while True:
        try:
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
                    break
        except (pika.exceptions.ChannelClosedByBroker, pika.exceptions.AMQPConnectionError):
            logging.warning("AMQP Error detected, retrying in 60s")
            sleep(60)
            continue
