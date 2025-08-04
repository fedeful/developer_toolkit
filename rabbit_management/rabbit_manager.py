#!/usr/bin/env python3
"""
Simple RabbitMQ Queue Cleaner with Kombu and Click
Supports delete, purge, and listing queues via the management API
"""

import click
import requests
from urllib.parse import urlparse
from kombu import Connection, Queue

def test_connection(connection_url):
    try:
        with Connection(connection_url) as conn:
            conn.ensure_connection(max_retries=3)
        return True
    except Exception as e:
        click.secho(f"Connection failed: {e}", fg="red")
        return False

def delete_queues(connection_url, queue_names, confirm=True):
    """Delete specified queues"""

    if not test_connection(connection_url):
        click.secho("Aborting due to connection failure.", fg="red")
        return

    if confirm:
        click.echo(f"About to delete {len(queue_names)} queues:")
        for name in queue_names:
            click.echo(f"  - {name}")

        if not click.confirm("\nProceed?"):
            click.echo("Cancelled.")
            return

    deleted = 0
    failed = 0

    try:
        with Connection(connection_url) as conn:
            for queue_name in queue_names:
                try:
                    queue = Queue(queue_name)
                    queue = queue.bind(conn.channel())

                    # Check if exists and get info
                    try:
                        method = queue.queue_declare(passive=True)
                        msg_count = method.message_count
                        click.echo(f"Deleting '{queue_name}' ({msg_count} messages)")
                    except NotFound:
                        click.echo(f"Queue '{queue_name}' not found, skipping")
                        continue

                    # Delete the queue
                    queue.delete()
                    click.secho(f"✅ Deleted: {queue_name}", fg="green")
                    deleted += 1

                except Exception as e:
                    click.secho(f"❌ Failed to delete {queue_name}: {e}", fg="red")
                    failed += 1

    except Exception as e:
        click.secho(f"Connection error: {e}", fg="red")
        return

    click.echo(f"\nResult: {deleted} deleted, {failed} failed")


def purge_queues(connection_url, queue_names):
    """Purge messages from queues without deleting them"""

    total_purged = 0

    try:
        with Connection(connection_url) as conn:
            for queue_name in queue_names:
                try:
                    queue = Queue(queue_name)
                    queue = queue.bind(conn.channel())

                    purged = queue.purge()
                    click.secho(f"✅ Purged {purged} messages from: {queue_name}", fg="green")
                    total_purged += purged

                except NotFound:
                    click.secho(f"⚠️  Queue not found: {queue_name}", fg="yellow")
                except Exception as e:
                    click.secho(f"❌ Error purging {queue_name}: {e}", fg="red")

    except Exception as e:
        click.secho(f"Connection error: {e}", fg="red")
        return

    click.echo(f"\nTotal messages purged: {total_purged}")


def get_all_queue_names(connection_url, vhost='/', user='guest', password='guest', host=None, port=15672):
    """
    Get all queue names from RabbitMQ management API
    """
    parsed = urlparse(connection_url)
    api_host = host or parsed.hostname or 'localhost'
    api_port = port or 15672
    api_vhost = vhost or parsed.path.lstrip('/') or '/'

    # Encode '/' vhost as '%2F'
    encoded_vhost = api_vhost.replace('/', '%2F')
    url = f"http://{api_host}:{api_port}/api/queues/{encoded_vhost}"

    try:
        response = requests.get(url, auth=(user, password))
        response.raise_for_status()
        queues = response.json()
        return [q['name'] for q in queues]
    except Exception as e:
        click.secho(f"Failed to fetch queues: {e}", fg="red")
        return []


@click.group()
def cli():
    """Simple RabbitMQ queue cleaner utility.

    Commands:
      - delete: Delete specified queues
      - purge: Purge messages from specified queues
      - list-queues: List queues via RabbitMQ HTTP API

    Examples:

    \b
      python simple_cleaner.py delete amqp://guest:guest@localhost// queue1,queue2
      python simple_cleaner.py purge amqp://guest:guest@localhost// queue1
      python simple_cleaner.py list-queues amqp://guest:guest@localhost//
    """
    pass


@cli.command()
@click.argument('connection_url')
@click.argument('queues')
@click.option('--yes', is_flag=True, help="Skip confirmation prompt when deleting queues.")
def delete(connection_url, queues, yes):
    """Delete specified QUEUES (comma-separated) from RabbitMQ server."""
    queue_names = [q.strip() for q in queues.split(',')]
    delete_queues(connection_url, queue_names, confirm=not yes)


@cli.command()
@click.argument('connection_url')
@click.argument('queues')
def purge(connection_url, queues):
    """Purge messages from specified QUEUES (comma-separated) without deleting them."""
    queue_names = [q.strip() for q in queues.split(',')]
    purge_queues(connection_url, queue_names)


@cli.command(name="list-queues")
@click.argument('connection_url')
@click.option('--user', default='guest', help="HTTP API username")
@click.option('--password', default='guest', help="HTTP API password")
@click.option('--vhost', default='/', help="RabbitMQ virtual host")
@click.option('--host', help="Override host in AMQP URL")
@click.option('--port', default=15672, help="RabbitMQ management API port")
def list_queues_cmd(connection_url, user, password, vhost, host, port):
    """List all queue names via RabbitMQ management HTTP API."""
    names = get_all_queue_names(connection_url, vhost, user, password, host, port)
    if names:
        click.echo(f"Found {len(names)} queues:")
        for name in names:
            click.echo(f"  - {name}")
    else:
        click.echo("No queues found or failed to connect.")


if __name__ == "__main__":
    cli()
