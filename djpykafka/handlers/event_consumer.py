import logging
import json
from typing import List, Optional, Union
from functools import wraps
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

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

    def __init__(self, bootstrap_servers: Union[str, List[str]], client_id: Optional[str] = None, group_id: Optional[str] = None, **kwargs) -> None:
        self.handlers = {}
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.group_id = group_id
        self._kwargs = kwargs
        self.__class__.consumers.append(self)

    def register_handler(self, topic: str, handler: callable):
        self.handlers[topic] = handler

    def _dispatch(self, message: ConsumerRecord):
        self.handlers[message.topic](str(message.value, 'utf-8'))

    def run(self):
        self.consumer = KafkaConsumer(
            *self.handlers.keys(),
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            group_id=self.group_id,
            **self._kwargs,
        )

        for message in self.consumer:
            self._dispatch(message)


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
