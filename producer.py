import csv
import logging
import json
import pika
import pika.channel

from tornado import gen
from tornado.queues import Queue

from apps.base.ioloop import IOLoop
from apps.base.pika import ConnectionManager


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Producer(object):

    _channel = None

    _exchange = 'user'
    _exchange_type = 'topic'
    _routing_key = 'user.new'

    _user_queue = Queue(maxsize=100)

    def setup(self, channel: pika.channel.Channel):
        """
        Invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        """
        logger.info('Channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self._on_channel_closed)
        self.setup_exchange(self._exchange)
        IOLoop.current().spawn_callback(gen.convert_yielded(self._user_consumer()))

    def setup_exchange(self, exchange_name):
        """
        Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str exchange_name: The name of the exchange to declare
        """
        logger.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(exchange=exchange_name, exchange_type=self._exchange_type)

    def _on_channel_closed(self, channel: pika.channel.Channel, reply_code: int, reply_text: str):
        """
        Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed
        """
        logger.warning('Channel %i was closed: (%s) %s', channel, reply_code, reply_text)

    async def send(self, data):
        """
        Send request action to the badges server
        """
        logger.info('Sent to the user queue: {}'.format(data))
        self._channel.basic_publish(
            exchange=self._exchange,
            routing_key=self._routing_key,
            body=json.dumps(data),
            properties=pika.BasicProperties(content_type='application/json', delivery_mode=2)
        )

    async def _user_consumer(self):
        """
        Base consumer of users from local queue to dedicated
        """
        async for user in self._user_queue:
            try:
                await self.send(user)
            finally:
                self._user_queue.task_done()

    async def add_user(self, user):
        """
        Add user to local queue, if it is full adding will be asynchronously wait resolving of it
        """
        await self._user_queue.put(user)

    async def fill_queue_from_csv(self, filename: str):
        """
        Read CSV file with users and add all of this list to a local queue
        """
        with open(filename) as users_csv:
            reader = csv.DictReader(users_csv, fieldnames=['fullname', 'email'])
            for user in reader:
                await self.add_user(user)


producer = Producer()
connection_manager = ConnectionManager().open_channel(producer.setup)


if __name__ == '__main__':
    ioloop = IOLoop.current()
    ioloop.spawn_callback(gen.convert_yielded(producer.fill_queue_from_csv('data.csv')))
    ioloop.start()
