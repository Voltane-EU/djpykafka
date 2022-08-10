import logging
from typing import Type, TypeVar
from functools import partial, cached_property
import warnings
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from kafka.producer.future import FutureRecordMetadata
from pydantic import BaseModel
from django.dispatch import receiver, Signal
from django.db.models.signals import post_save, post_delete
from django.db import models
from sentry_sdk import Hub, start_span
from django.db.utils import OperationalError
from django.utils import timezone
from sentry_tools.decorators import instrument_span, capture_exception
from djdantic.utils.pydantic_django import transfer_from_orm
from djdantic.utils.typing import with_typehint
from djutils.transaction import on_transaction_complete
from dirtyfields import DirtyFieldsMixin

from ..schemas import DataChangeEvent, EventMetadata, Version
from ..models import KafkaPublishMixin


TBaseModel = TypeVar('TBaseModel', bound=BaseModel)
TDjangoModel = TypeVar('TDjangoModel', bound=models.Model)


class EventPublisher:
    action = 'update'

    def __init_subclass__(
        cls,
        orm_model: Type[TDjangoModel],
        event_schema: Type[TBaseModel],
        connection: KafkaProducer,
        topic: str,
        data_type: str,
        is_changed_included: bool = False,
        version: Version = (1, 0, 0),
        type: str = 'data',
        **kwargs,
    ):
        super().__init_subclass__()
        cls.connection = connection

        cls.topic = topic
        cls.orm_model = orm_model
        cls.event_schema = event_schema
        cls.data_type = data_type
        cls.version = version
        cls.type = type
        cls.is_changed_included = is_changed_included
        cls.is_tenant_bound = hasattr(cls.orm_model, 'tenant_id')
        cls.logger = logging.getLogger(f'{cls.__module__}.{cls.__name__}')
        cls._kwargs = kwargs

        cls.register()
        cls.logger.info("Registered EventPublisher %s", cls.__name__)

        if not issubclass(cls.orm_model, KafkaPublishMixin):
            warnings.warn("Using EventPublisher with a model that doesn't has the djpykafka.models.KafkaPublishMixin is not recommended", stacklevel=2)

    @classmethod
    def register(cls):
        raise NotImplementedError

    @classmethod
    @on_transaction_complete()
    @instrument_span(
        op='EventPublisher',
        description=lambda cls, sender, instance, signal, **kwargs: f'{cls} for {instance} via {signal}',
    )
    def handle(cls, sender, instance: TDjangoModel, signal: Signal, **kwargs):
        cls.logger.debug("%s.handle from %s with %s for %s", cls, sender, signal, instance)
        if kwargs.get('update_fields') and 'last_kafka_publish_at' in kwargs['update_fields']:
            cls.logger.debug("discarding signal as last_kafka_publish_at is set in update_fields")
            return

        instance = cls(sender, instance, signal, **kwargs)
        capture_exception(instance.process)()

    def __init__(self, sender, instance: TDjangoModel, signal: Signal, **kwargs):
        self.sender = sender
        self.instance = instance
        self.signal = signal
        self.kwargs = kwargs

    @cached_property
    def metadata(self) -> EventMetadata:
        return EventMetadata(
            version=self.version,
        )

    @cached_property
    def data_op(self) -> DataChangeEvent.DataOperation:
        return DataChangeEvent.DataOperation.UPDATE

    def get_data(self) -> dict:
        return transfer_from_orm(self.event_schema, self.instance).dict(by_alias=True)

    @cached_property
    def modified_fields(self) -> dict:
        if isinstance(self.instance, DirtyFieldsMixin):
            return self.instance.get_dirty_fields(check_relationship=True)

        else:
            warnings.warn("Cannot access get_dirty_fields method. The DirtyFieldsMixin (from django-dirtyfields) is not added to your model.")

        return {}

    @cached_property
    def is_modified(self) -> bool:
        if isinstance(self.instance, DirtyFieldsMixin):
            return bool(self.modified_fields)

        return True

    def get_body(self) -> DataChangeEvent:
        data = self.get_data()

        if self.is_changed_included:
            data['_changed'] = [
                {
                    'name': field,
                } for field, _value in self.modified_fields.items()
            ]

        return DataChangeEvent(
            data=data,
            data_type=self.data_type,
            data_op=self.data_op,
            tenant_id=self.instance.tenant_id if self.is_tenant_bound else None,
            metadata=self.metadata,
        )

    def get_headers(self):
        return {
            'data_type': self.data_type,
            'data_op': self.data_op.value,
            'record_id': self.instance.id,
            'tenant_id': self.instance.tenant_id if self.is_tenant_bound else None,
            'event_id': self.metadata.eid,
            'flow_id': self.metadata.flow_id,
            'version': str(self.metadata.version) if self.metadata.version else None,
        }

    @property
    def message_key(self):
        return bytes(f'{self.data_type}[{self.instance.id}]', 'utf-8')

    @instrument_span(op='EventPublisher.get_message_data')
    def get_message_data(self):
        return {
            'topic': self.topic,
            'key': self.message_key,
            'headers': [(key, bytes(value, 'utf-8')) for key, value in self.get_headers().items() if value],
            'value': bytes(self.get_body().json(), 'utf-8'),
        }

    def send_callback(self, value):
        self.instance.last_kafka_publish_at = timezone.now()
        self.instance.save(update_fields=['last_kafka_publish_at'])

    def error_callback(self, error):
        self.logger.exception(error)

    def process(self):
        span = Hub.current.scope.span
        span.set_tag('topic', self.topic)
        span.set_tag('sender', self.sender)
        span.set_tag('signal', self.signal)
        span.set_tag('orm_model', self.orm_model)

        self.logger.debug("Publish DataChangeEvent for %s with schema %s on %r", self.orm_model, self.event_schema, self.topic)
        if not self.is_modified:
            self.logger.debug("Not publishing DataChangeEvent, not modified")
            return

        data = self.get_message_data()  # prepare data to allow measuring just self.connection.send

        with start_span(op='KafkaProducer.send'):
            future_message: FutureRecordMetadata = self.connection.send(**data)

        if issubclass(self.orm_model, KafkaPublishMixin) and self.data_op != DataChangeEvent.DataOperation.DELETE:
            future_message.add_callback(self.send_callback)
            future_message.add_errback(self.error_callback)

        else:
            with start_span(op='FutureRecordMetadata.get'):
                try:
                    future_message.get()

                except KafkaTimeoutError as error:
                    raise OperationalError from error

        return future_message


class DataChangePublisher(with_typehint(EventPublisher)):
    @property
    def action(self):
        if self.signal == post_save:
            return 'create' if self.kwargs.get('created') else 'update'

        elif self.signal == post_delete:
            return 'delete'

    @cached_property
    def data_op(self) -> DataChangeEvent.DataOperation:
        return getattr(DataChangeEvent.DataOperation, self.action.upper())

    @classmethod
    def register(cls):
        cls._handle_post_save = partial(cls.handle, signal=post_save)
        receiver(post_save, sender=cls.orm_model)(cls._handle_post_save)

        if cls._kwargs.get('is_post_delete_received', True):
            cls._handle_post_delete = partial(cls.handle, signal=post_delete)
            receiver(post_delete, sender=cls.orm_model)(cls._handle_post_delete)

        cls.logger.debug("Registered post_save + post_delete handlers for %s", cls.orm_model)


class StatusChangePublisher(with_typehint(EventPublisher)):
    @property
    def action(self):
        return str(self.instance.status).lower()

    @classmethod
    def register(cls):
        cls._handle_status_change = partial(cls.handle, signal=cls.orm_model.STATUS_CHANGE)
        receiver(cls.orm_model.STATUS_CHANGE, sender=cls.orm_model)(cls._handle_status_change)

        cls.logger.debug("Registered status_change handlers for %s", cls.orm_model)
