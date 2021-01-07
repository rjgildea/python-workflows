import configparser
import json
import threading
import time

import pika
import stomp
import workflows
from workflows.transport.common_transport import CommonTransport


class RabbitTransport(CommonTransport):
    """Abstraction layer for messaging infrastructure. Here we are using ActiveMQ
    with STOMP."""

    # Set some sensible defaults
    defaults = {
        "--rabbit-host": "localhost",
        "--rabbit-port": 61613,
        "--rabbit-user": "admin",
        "--rabbit-pass": "password",
        "--rabbit-prfx": "",
    }
    # Effective configuration
    config = {}

    def __init__(self):
        self._connected = False
        self._namespace = ""
        self._idcounter = 0
        self._lock = threading.RLock()
        self._stomp_listener = stomp.listener.ConnectionListener()
        #   self._stomp_listener = stomp.PrintingListener()
        self._stomp_listener.on_message = self._on_message
        self._stomp_listener.on_before_message = lambda x, y: (x, y)

    def get_namespace(self):
        """Return the stomp namespace. This is a prefix used for all topic and
        queue names."""
        if self._namespace.endswith("."):
            return self._namespace[:-1]
        return self._namespace

    @classmethod
    def load_configuration_file(cls, filename):
        cfgparser = configparser.ConfigParser(allow_no_value=True)
        if not cfgparser.read(filename):
            raise workflows.Error(
                "Could not read from configuration file %s" % filename
            )
        for cfgoption, target in [
            ("host", "--rabbit-host"),
            ("port", "--rabbit-port"),
            ("password", "--rabbit-pass"),
            ("username", "--rabbit-user"),
            ("prefix", "--rabbit-prfx"),
        ]:
            try:
                cls.defaults[target] = cfgparser.get("rabbit", cfgoption)
            except configparser.NoOptionError:
                pass

    @classmethod
    def add_command_line_options(cls, parser):
        """function to inject command line parameters"""
        if "add_argument" in dir(parser):
            return cls.add_command_line_options_argparse(parser)
        else:
            return cls.add_command_line_options_optparse(parser)

    @classmethod
    def add_command_line_options_argparse(cls, argparser):
        """function to inject command line parameters into
        a Python ArgumentParser."""
        import argparse

        class SetParameter(argparse.Action):
            """callback object for ArgumentParser"""

            def __call__(self, parser, namespace, value, option_string=None):
                cls.config[option_string] = value
                if option_string == "--rabbit-conf":
                    cls.load_configuration_file(value)

        argparser.add_argument(
            "--rabbit-host",
            metavar="HOST",
            default=cls.defaults.get("--rabbit-host"),
            help="Stomp broker address, default '%(default)s'",
            type=str,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-port",
            metavar="PORT",
            default=cls.defaults.get("--rabbit-port"),
            help="Stomp broker port, default '%(default)s'",
            type=int,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-user",
            metavar="USER",
            default=cls.defaults.get("--rabbit-user"),
            help="Stomp user, default '%(default)s'",
            type=str,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-pass",
            metavar="PASS",
            default=cls.defaults.get("--rabbit-pass"),
            help="Stomp password",
            type=str,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-prfx",
            metavar="PRE",
            default=cls.defaults.get("--rabbit-prfx"),
            help="Stomp namespace prefix, default '%(default)s'",
            type=str,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-conf",
            metavar="CNF",
            default=cls.defaults.get("--rabbit-conf"),
            help="Stomp configuration file containing connection information, disables default values",
            type=str,
            action=SetParameter,
        )

    @classmethod
    def add_command_line_options_optparse(cls, optparser):
        """function to inject command line parameters into
        a Python OptionParser."""

        def set_parameter(option, opt, value, parser):
            """callback function for OptionParser"""
            cls.config[opt] = value
            if opt == "--rabbit-conf":
                cls.load_configuration_file(value)

        optparser.add_option(
            "--rabbit-host",
            metavar="HOST",
            default=cls.defaults.get("--rabbit-host"),
            help="Stomp broker address, default '%default'",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-port",
            metavar="PORT",
            default=cls.defaults.get("--rabbit-port"),
            help="Stomp broker port, default '%default'",
            type="int",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-user",
            metavar="USER",
            default=cls.defaults.get("--rabbit-user"),
            help="Stomp user, default '%default'",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-pass",
            metavar="PASS",
            default=cls.defaults.get("--rabbit-pass"),
            help="Stomp password",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-prfx",
            metavar="PRE",
            default=cls.defaults.get("--rabbit-prfx"),
            help="Stomp namespace prefix, default '%default'",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-conf",
            metavar="CNF",
            default=cls.defaults.get("--rabbit-conf"),
            help="Stomp configuration file containing connection information, disables default values",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )

    def connect(self):
        with self._lock:
            if self._connected:
                return True
            self._conn = pika.BlockingConnection(pika.ConnectionParameters(host=self.defaults.get("--rabbit-host")))
            self._channel = self._conn.channel()
#            self._channel.queue_declare(queue='load.destination', durable=True)
#            self._channel.queue_declare(queue='load.whatever', durable=True)
#            self._channel.queue_declare(queue='load.done', durable=True)

#            self._conn.set_listener("", self._stomp_listener)
#            username = self.config.get(
#                "--rabbit-user", self.defaults.get("--rabbit-user")
#            )
#            password = self.config.get(
#                "--rabbit-pass", self.defaults.get("--rabbit-pass")
#            )
#            timeout = time.time() + 30
#            connection_failure = threading.Event()
#            handler_old = self._stomp_listener.on_disconnected
#            self._stomp_listener.on_disconnected = lambda: connection_failure.set()
#            try:
#                if username or password:
#                    self._conn.connect(username, password, wait=False)
#                else:  # anonymous access
#                    self._conn.connect(wait=False)
#            except stomp.exception.ConnectFailedException:
#                raise workflows.Disconnected(
#                    "Could not initiate connection to stomp host"
#                )
#            while (
#                time.time() < timeout
#                and not self._conn.is_connected()
#                and not connection_failure.is_set()
#            ):
#                time.sleep(0.02)
#            self._stomp_listener.on_disconnected = handler_old
#            if connection_failure.is_set() or not self._conn.is_connected():
#                raise workflows.Disconnected(
#                    "Could not initiate connection to stomp host"
#                )
#            self._namespace = self.config.get(
#                "--rabbit-prfx", self.defaults.get("--rabbit-prfx")
#            )
            if self._namespace and not self._namespace.endswith("."):
                self._namespace = self._namespace + "."
            self._connected = True
        return True

    def is_connected(self):
        """Return connection status"""
        self._connected = self._connected and self._conn.is_connected()
        return self._connected

    def disconnect(self):
        """Gracefully close connection to stomp server."""
        if self._connected:
            self._connected = False
            self._conn.close()

    def broadcast_status(self, status):
        """Broadcast transient status information to all listeners"""
        self._broadcast(
            "transient.status",
            json.dumps(status),
            headers={"expires": str(int((15 + time.time()) * 1000))},
        )

    def _subscribe(self, sub_id, channel, callback, **kwargs):
        """Listen to a queue, notify via callback function.
        :param sub_id: ID for this subscription in the transport layer
        :param channel: Queue name to subscribe to
        :param callback: Function to be called when messages are received
        :param **kwargs: Further parameters for the transport layer. For example
          acknowledgement:  If true receipt of each message needs to be
                            acknowledged.
          exclusive:        Attempt to become exclusive subscriber to the queue.
          ignore_namespace: Do not apply namespace to the destination name
          priority:         Consumer priority, messages are sent to higher
                            priority consumers whenever possible.
          selector:         Only receive messages filtered by a selector. See
                            https://activemq.apache.org/activemq-message-properties.html
                            for potential filter criteria. Uses SQL 92 syntax.
          transformation:   Transform messages into different format. If set
                            to True, will use 'jms-object-json' formatting.
        """

        def _redirect_callback(ch, method, properties, body):
            callback({"message-id":method.delivery_tag}, body.decode())

        self._channel.basic_qos(prefetch_count=1)
        auto_ack = not kwargs.get("acknowledgement")
        self._channel.basic_consume(queue=channel, on_message_callback=_redirect_callback, auto_ack=auto_ack)
        return

        headers = {}
        if kwargs.get("exclusive"):
            headers["activemq.exclusive"] = "true"
        if kwargs.get("ignore_namespace"):
            destination = "/queue/" + channel
        else:
            destination = "/queue/" + self._namespace + channel
        if kwargs.get("priority"):
            headers["activemq.priority"] = kwargs["priority"]
        if kwargs.get("retroactive"):
            headers["activemq.retroactive"] = "true"
        if kwargs.get("selector"):
            headers["selector"] = kwargs["selector"]
        if kwargs.get("transformation"):
            if kwargs["transformation"] is True:
                headers["transformation"] = "jms-object-json"
            else:
                headers["transformation"] = kwargs["transformation"]
        if kwargs.get("acknowledgement"):
            ack = "client-individual"
        else:
            ack = "auto"

        self._conn.subscribe(destination, sub_id, headers=headers, ack=ack)

    def _subscribe_broadcast(self, sub_id, channel, callback, **kwargs):
        """Listen to a broadcast topic, notify via callback function.
        :param sub_id: ID for this subscription in the transport layer
        :param channel: Topic name to subscribe to
        :param callback: Function to be called when messages are received
        :param **kwargs: Further parameters for the transport layer. For example
          ignore_namespace: Do not apply namespace to the destination name
          retroactive:      Ask broker to send old messages if possible
          transformation:   Transform messages into different format. If set
                            to True, will use 'jms-object-json' formatting.
        """
        headers = {}
        if kwargs.get("ignore_namespace"):
            destination = "/topic/" + channel
        else:
            destination = "/topic/" + self._namespace + channel
        if kwargs.get("retroactive"):
            headers["activemq.retroactive"] = "true"
        if kwargs.get("transformation"):
            if kwargs["transformation"] is True:
                headers["transformation"] = "jms-object-json"
            else:
                headers["transformation"] = kwargs["transformation"]
        self._conn.subscribe(destination, sub_id, headers=headers)

    def _unsubscribe(self, subscription, **kwargs):
        """Stop listening to a queue or a broadcast
        :param subscription: Subscription ID to cancel
        """
        self._conn.unsubscribe(id=subscription)
        # Callback reference is kept as further messages may already have been received

    def _send(
        self, destination, message, headers=None, delay=None, expiration=None, **kwargs
    ):
        """Send a message to a queue.
        :param destination: Queue name to send to
        :param message: A string to be sent
        :param **kwargs: Further parameters for the transport layer. For example
          delay:            Delay transport of message by this many seconds
          expiration:       Optional expiration time, relative to sending time
          headers:          Optional dictionary of header entries
          ignore_namespace: Do not apply namespace to the destination name
          persistent:       Whether to mark messages as persistent, to be kept
                            between broker restarts. Default is 'true'.
          transaction:      Transaction ID if message should be part of a
                            transaction
        """
        self._channel.basic_publish(
    exchange='',
    routing_key=destination,
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=2,  # make message persistent
    ))
        return

        if not headers:
            headers = {}
        if "persistent" not in headers:
            headers["persistent"] = "true"
        if delay:
            # The 'delay' mechanism is only supported when
            # schedulerSupport is enabled on the broker.
            headers["AMQ_SCHEDULED_DELAY"] = int(1000 * delay)
        if expiration:
            headers["expires"] = int((time.time() + expiration) * 1000)
        if kwargs.get("ignore_namespace"):
            destination = "/queue/" + destination
        else:
            destination = "/queue/" + self._namespace + destination
        try:
            self._conn.send(destination, message, headers=headers, **kwargs)
        except stomp.exception.NotConnectedException:
            self._connected = False
            raise workflows.Disconnected("No connection to stomp host")



    def _broadcast(
        self, destination, message, headers=None, delay=None, expiration=None, **kwargs
    ):
        """Broadcast a message.
        :param destination: Topic name to send to
        :param message: A string to be broadcast
        :param **kwargs: Further parameters for the transport layer. For example
          delay:            Delay transport of message by this many seconds
          expiration:       Optional expiration time, relative to sending time
          headers:          Optional dictionary of header entries
          ignore_namespace: Do not apply namespace to the destination name
          transaction:      Transaction ID if message should be part of a
                            transaction
        """
        if not headers:
            headers = {}
        if delay:
            headers["AMQ_SCHEDULED_DELAY"] = 1000 * delay
        if expiration:
            headers["expires"] = int((time.time() + expiration) * 1000)
        if kwargs.get("ignore_namespace"):
            destination = "/topic/" + destination
        else:
            destination = "/topic/" + self._namespace + destination
        try:
            self._conn.send(destination, message, headers=headers, **kwargs)
        except stomp.exception.NotConnectedException:
            self._connected = False
            raise

    def _transaction_begin(self, transaction_id, **kwargs):
        """Start a new transaction.
        :param transaction_id: ID for this transaction in the transport layer.
        :param **kwargs: Further parameters for the transport layer.
        """
        self._channel.tx_select()

    def _transaction_abort(self, transaction_id, **kwargs):
        """Abort a transaction and roll back all operations.
        :param transaction_id: ID of transaction to be aborted.
        :param **kwargs: Further parameters for the transport layer.
        """
        self._channel.tx_rollback()

    def _transaction_commit(self, transaction_id, **kwargs):
        """Commit a transaction.
        :param transaction_id: ID of transaction to be committed.
        :param **kwargs: Further parameters for the transport layer.
        """
        self._channel.tx_commit()

    def _ack(self, message_id, subscription_id, **kwargs):
        """Acknowledge receipt of a message. This only makes sense when the
        'acknowledgement' flag was set for the relevant subscription.
        :param message_id: ID of the message to be acknowledged
        :param subscription: ID of the relevant subscriptiong
        :param **kwargs: Further parameters for the transport layer. For example
               transaction: Transaction ID if acknowledgement should be part of
                            a transaction
        """
        self._channel.basic_ack(delivery_tag=message_id)

    def _nack(self, message_id, subscription_id, **kwargs):
        """Reject receipt of a message. This only makes sense when the
        'acknowledgement' flag was set for the relevant subscription.
        :param message_id: ID of the message to be rejected
        :param subscription: ID of the relevant subscriptiong
        :param **kwargs: Further parameters for the transport layer. For example
               transaction: Transaction ID if rejection should be part of a
                            transaction
        """
        self._conn.nack(message_id, subscription_id, **kwargs)

    @staticmethod
    def _mangle_for_sending(message):
        """Function that any message will pass through before it being forwarded to
        the actual _send* functions.
        Stomp only deals with serialized strings, so serialize message as json.
        """
        return json.dumps(message)

    @staticmethod
    def _mangle_for_receiving(message):
        """Function that any message will pass through before it being forwarded to
        the receiving subscribed callback functions.
        This transport class only deals with serialized strings, so decode
        message from json. However anything can come into here, so catch any
        deserialization errors.
        """
        try:
            return json.loads(message)
        except (TypeError, ValueError):
            return message

    ## Stomp listener methods #####################################################

    def _on_message(self, headers, body):
        subscription_id = int(headers.get("subscription"))
        target_function = self.subscription_callback(subscription_id)
        if target_function:
            target_function(headers, body)
        else:
            raise workflows.Error(
                "Unhandled message {} {}".format(repr(headers), repr(body))
            )
