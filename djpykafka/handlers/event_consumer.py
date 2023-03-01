import logging
import json
from threading import Thread
from secrets import token_hex
from time import sleep
from typing import Callable, Dict, List, Literal, Optional, Union
from collections import defaultdict
from functools import wraps
from kafka import KafkaConsumer
from kafka.coordinator.consumer import ConsumerCoordinator
from kafka.consumer.fetcher import ConsumerRecord
from django.conf import settings

try:
    from sentry_sdk.integrations.serverless import serverless_function
    from sentry_sdk import last_event_id, Hub
    from sentry_sdk.tracing import Transaction

except ImportError:
    Hub = None


_logger = logging.getLogger(__name__)


class Consumer:
    consumers = []

    @classmethod
    def get_consumer(cls):
        if len(cls.consumers) != 1:
            raise ValueError('more than one consumer defined')

        return cls.consumers[0]

    def __init__(self, bootstrap_servers: Optional[Union[str, List[str]]] = None, client_id: Optional[str] = None, group_id: Optional[str] = None, auto_offset_reset: Literal['earliest', 'latest'] = 'earliest', **kwargs) -> None:
        self.handlers: defaultdict[str, List[Callable[[str], None]]] = defaultdict(list)
        self.bootstrap_servers = getattr(settings, 'BROKER_URL', bootstrap_servers)
        if not self.bootstrap_servers:
            raise ValueError('bootstrap_servers or settings.BROKER_URL must be given')

        self.client_id = (f'{client_id}-' if client_id else '') + hex(id(self))[2:]
        self.group_id = group_id
        self._kwargs = {
            'auto_offset_reset': getattr(settings, 'BROKER_AUTO_OFFSET_RESET', auto_offset_reset),
            'request_timeout_ms': getattr(settings, 'BROKER_REQUEST_TIMEOUT', None),
            'session_timeout_ms': getattr(settings, 'BROKER_SESSION_TIMEOUT', None),
            'security_protocol': getattr(settings, 'BROKER_SECURITY_PROTOCOL', None),
            'sasl_mechanism': getattr(settings, 'BROKER_SASL_MECHANISM', None),
            'sasl_plain_username': getattr(settings, 'BROKER_SASL_PLAIN_USERNAME', None),
            'sasl_plain_password': getattr(settings, 'BROKER_SASL_PLAIN_PASSWORD', None),
            'ssl_cafile': getattr(settings, 'BROKER_SSL_CERTFILE', None),
            'max_poll_records': getattr(settings, 'BROKER_MAX_POLL_RECORDS', None),
            'max_poll_interval_ms': getattr(settings, 'BROKER_MAX_POLL_INTERVAL_MS', None),
            **kwargs,
        }
        self.__class__.consumers.append(self)
        self.kafka_consumers: Dict[str, KafkaConsumer] = {}
        self.threads: Dict[str, Thread] = {}
        self.logger = logging.getLogger('djpykafka.event')

    def register_handler(self, topic: str, handler: callable):
        self.handlers[topic].append(handler)

    def _dispatch(self, message: ConsumerRecord):
        self.logger.info("%s %s offset=%s partition=%s size=%s key=%s", message.topic, message.timestamp, message.offset, message.partition, message.serialized_value_size, message.key)
        for handler in self.handlers[message.topic]:
            handler(str(message.value, 'utf-8'))

    def run(self):
        for topic in self.handlers.keys():
            self.kafka_consumers[topic] = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                client_id=f'{self.client_id}-{token_hex(4)}-{topic}',
                group_id=self.group_id,
                **self._kwargs,
            )

            self.threads[topic] = Thread(target=self.consume_messages, kwargs={'topic': topic}, daemon=True)
            self.threads[topic].start()

        while True:
            if not all(thread.is_alive() for thread in self.threads.values()):
                break

            for consumer in self.kafka_consumers.values():
                consumer._coordinator.ensure_active_group()

            sleep(5)

    def consume_messages(self, topic: str):
        message: ConsumerRecord
        for message in self.kafka_consumers[topic]:
            tries: int = 0
            _prev_error: Optional[Exception] = None
            while True:
                tries += 1
                try:
                    self._dispatch(message)

                except Exception as error:
                    _prev_error = error
                    if not _prev_error or str(error) != str(_prev_error):
                        self.logger.exception(error)

                    if tries >= 25:
                        sleep(5)

                    else:
                        sleep(tries ** 1.425 * 0.05)

                else:
                    break


def transaction_captured_function(func, transaction_name: Optional[str] = None):
    @wraps(func)
    def wrapper(*args, **kwargs):
        flow_id = data = None

        try:
            if isinstance(args[0], (str, bytes)):
                try:
                    data = json.loads(args[0])

                except json.JSONDecodeError:
                    pass

            elif isinstance(args[0], dict):
                data = args[0]

            if data:
                flow_id = data.get('metadata', {}).get('flow_id')

        except IndexError:
            pass

        transaction = Transaction.continue_from_headers(
            {'sentry-trace': flow_id},
            op='message_handler',
            name=transaction_name or f'{func.__module__}.{func.__name__}',
        )
        with Hub.current.start_transaction(transaction):
            result = func(*args, **kwargs)

        _logger.debug("Logged message handling with trace_id=%s, span_id=%s, id=%s", transaction.trace_id, transaction.span_id, last_event_id())
        return result

    return wrapper


def _message_handler(
    topic: str,
    consumer: Optional[Consumer],
):
    if not consumer:
        consumer = Consumer.get_consumer()

    def wrapper(func):
        consumer.register_handler(topic=topic, handler=func)

        return func

    return wrapper


if Hub:
    def message_handler(
        topic: str,
        consumer: Optional[Consumer] = None,
        transaction_name: Optional[str] = None
    ):
        def decorator(func):
            return _message_handler(
                topic=topic,
                consumer=consumer,
            )(serverless_function(transaction_captured_function(func, transaction_name=transaction_name)))

        return decorator

else:
    message_handler = _message_handler
