import click
from prometheus_client import start_http_server

from copybot import __version__, rabbitmq_message_get, rabbitmq_message_put


@click.group()
def cli():
    pass


@cli.command(help="Prints the current version.")
def version() -> None:
    print(__version__)


@cli.command(help="Run the application in consume mode.")
@click.option("-q", "--queue", default="clickhouse_testing", help="The RabbitMQ Queue to Use", show_default=True)
def consume(queue) -> None:
    start_http_server(9091)
    rabbitmq_message_get(queue=queue)


@cli.command(help="Pushes fake events to RabbitMQ for testing the end-to-end flow, do not use this in prod.")
@click.option("-c", "--count", default=10, help="Number of messages to push", show_default=True)
@click.option("-q", "--queue", default="clickhouse_testing", help="The RabbitMQ Queue to Use", show_default=True)
def push(count, queue) -> None:
    rabbitmq_message_put(count=count, queue=queue)


if __name__ == "__main__":
    cli()