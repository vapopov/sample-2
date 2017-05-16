import logging
import pika
import pika.channel
import abc

from typing import Optional
from functools import partial

from apps.options import settings


__all__ = ('ConnectionManager', 'ConsumerABC')


logger = logging.getLogger(__name__)


class ConnectionManager:

    _closing = False

    def __init__(self, amqp_url=settings.RABBITMQ_URL):
        self._url = amqp_url
        self._callbacks = []
        self._connection = self._connect()

    @property
    def raw_connection(self):
        """
        Pika connection instance
        """
        return self._connection

    @property
    def is_active(self):
        return self._connection.connection_state == self._connection.CONNECTION_OPEN

    def _connect(self) -> pika.TornadoConnection:
        """
        This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.
        """
        logger.info('Set Up connection to: {}'.format(self._url))
        return pika.TornadoConnection(pika.URLParameters(self._url), self._on_connection_open)

    def _on_connection_open(self, unused_connection: pika.SelectConnection):
        """
        This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.
        """
        logger.info('Connecting to %s', self._url)
        self._connection.add_on_close_callback(self._on_connection_closed)
        for callback in self._callbacks:
            callback()

    def open_channel(self, on_channel_open):
        """
        Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        if self.is_active:
            self._connection.channel(on_open_callback=on_channel_open)
        else:
            self._callbacks.append(partial(self.open_channel, on_channel_open))
        return self

    def reconnect(self):
        """
        Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.
        """
        if not self._closing:
            try:
                self._connection = self._connect()
            except Exception as e:
                logger.info(e)
                self._connection.add_timeout(5, self.reconnect)

    def close(self):
        """
        Closes the connection to RabbitMQ.
        """
        self._connection.close()

    def _on_connection_closed(self, connection: pika.connection.Connection, reply_code: int, reply_text: str):
        """
        Invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to RabbitMQ if it disconnects.
        """
        self._channel = None
        logger.warning('Connection closed, reopening in 5 seconds: (%s) %s', reply_code, reply_text)
        self._connection.add_timeout(5, self.reconnect)


class ConsumerABC(abc.ABC):

    _exchange = ''
    _routing_key = ''

    _consumer_tag = None

    _prefetch_count = 0
    _prefetch_size = 0

    @abc.abstractproperty
    def _exchange(self) -> Optional[str]:
        """
        Exchanges are AMQP entities where messages are sent.
        Exchanges take a message and route it into zero or more queues.
        The routing algorithm used depends on the exchange type and rules called bindings.
        """

    @abc.abstractproperty
    def _exchange_type(self) -> Optional[str]:
        """
        Exchange type to message spreading: direct, fanout, topic, headers
        """

    @abc.abstractproperty
    def _queue(self) -> str:
        """
        Queue with consuming/delivering messages
        """

    def __init__(self):
        """
        Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.
        """
        self._channel = None

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

    def setup(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object
        """
        logger.info('Channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self._on_channel_closed)
        self.setup_exchange(self._exchange)

    def setup_exchange(self, exchange_name):
        """
        Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str exchange_name: The name of the exchange to declare
        """
        logger.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self._on_exchange_declareok, exchange_name, self._exchange_type)

    def _on_exchange_declareok(self, unused_frame):
        """
        Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        """
        logger.info('Exchange declared')
        self.setup_queue(self._queue)

    def setup_queue(self, queue_name: str):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str queue_name: The name of the queue to declare.
        """
        logger.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(self._on_queue, queue_name, durable=True)

    def _on_queue(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame
        """
        logger.info('Binding %s to %s with %s',
                    self._exchange, self._queue, self._routing_key)
        self._channel.queue_bind(self._on_bindok, self._queue, self._exchange, self._routing_key)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.
        """
        logger.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)

    def _on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        logger.info('Consumer was cancelled remotely, shutting down: %r', method_frame)
        if self._channel:
            self._channel.close()

    @abc.abstractmethod
    def _on_message(self, unused_channel, basic_deliver, properties, body: str):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str body: The message body
        """

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame
        """
        logger.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def _start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.
        """
        logger.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._channel.basic_qos(prefetch_count=self._prefetch_count, prefetch_size=self._prefetch_size)
        self._consumer_tag = self._channel.basic_consume(self._on_message, self._queue)

    def _on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame
        """
        logger.info('Queue bound')
        self._start_consuming()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        logger.info('Closing the channel')
        self._channel.close()

    def stop(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self._channel:
            logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

