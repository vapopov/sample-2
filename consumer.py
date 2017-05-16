import logging
import json
import psycopg2

from tornado.ioloop import IOLoop
from tornado import gen

from apps.base.pika import ConsumerABC, ConnectionManager
from apps.base.pg_pools import Connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TopicConsumer(ConsumerABC):

    _exchange = 'user'
    _exchange_type = 'topic'
    _queue = 'user'
    _routing_key = 'user.new'

    _prefetch_count = 50

    _ioloop = IOLoop.current()

    async def _store_user(self, user):
        logger.info('Added to database: {}'.format(user))
        try:
            await Connection.pool.execute(
                "INSERT INTO users (email, fullname, times_received) VALUES (%s, %s, %s);",
                (user.get('email'), user.get('fullname'), 0)
            )
        except psycopg2.IntegrityError:
            await Connection.pool.execute(
                "UPDATE users SET times_received = times_received + 1 WHERE email = %s",
                (user.get('email'),)
            )

    def _on_message(self, unused_channel, basic_deliver, properties, body: bytes):
        result = json.loads(body.decode())

        future = gen.convert_yielded(self._store_user(result))

        def _result_callback(future):
            future.result()
            self._channel.basic_ack(basic_deliver.delivery_tag)

        self._ioloop.add_future(future, _result_callback)


if __name__ == '__main__':
    client = TopicConsumer()
    ConnectionManager().open_channel(client.setup)
    IOLoop.current().start()
