import logging
from typing import Callable, Generator, Iterator, List, Optional, Type, TypeVar
from functools import partial, cached_property
import warnings
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from kafka.producer.future import FutureRecordMetadata
from pydantic import BaseModel
from pydantic.fields import SHAPE_SINGLETON, SHAPE_LIST
from django.dispatch import receiver, Signal
from django.db.models.signals import post_save, post_delete
from django.db.models.fields.related_descriptors import ReverseManyToOneDescriptor
from django.db.models.fields import Field as DjangoField
from django.db import models
from sentry_sdk import Hub, start_span
from django.db.utils import OperationalError
from django.utils import timezone
from sentry_tools.decorators import instrument_span, capture_exception
from djdantic.utils.pydantic_django import transfer_from_orm
from djdantic.utils.typing import with_typehint
from djdantic.utils.pydantic import get_orm_field_attr
from djutils.transaction import on_transaction_complete
from dirtyfields import DirtyFieldsMixin

from ..schemas import DataChangeEvent, EventMetadata, Version
from ..models import KafkaPublishMixin


TBaseModel = TypeVar('TBaseModel', bound=BaseModel)
TDjangoModel = TypeVar('TDjangoModel', bound=models.Model)


class EventPublisher:
    action = 'update'

    orm_model: Type[TDjangoModel]
    event_schema: Type[TBaseModel]
    connection: KafkaProducer
    topic: str
    data_type: str
    is_changed_included: bool
    version: Version
    type: str
    submodels: List[str]

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
        submodels: Optional[List[str]] = None,
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
        cls.submodels = submodels or []
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
        on_transaction_complete()(capture_exception(instance.process))()

    def __init__(self, sender, instance: TDjangoModel, signal: Signal, **kwargs):
        self.sender = sender
        self.instance = instance
        self.signal = signal
        self.kwargs = kwargs

        if not self.instance.id:
            raise ValueError('instance must have an id')

        if self.is_tenant_bound and not self.instance.tenant_id:
            raise ValueError('Tenant bound instance must have a tenant set')

        self.data = self.get_message_data()  # prepare data to allow measuring just self.connection.send
        self.is_modified = self.check_if_modified()

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
    def _relevant_model_fields(self) -> Iterator[DjangoField]:
        def get_fields(model: BaseModel):
            for key, field in model.__fields__.items():
                if field.shape != SHAPE_SINGLETON:
                    continue

                if issubclass(field.type_, BaseModel):
                    yield from get_fields(field.type_)
                    continue

                orm_field = get_orm_field_attr(field.field_info, 'orm_field')
                if not orm_field:
                    continue

                yield orm_field.field

        yield from get_fields(self.event_schema)

    @cached_property
    def modified_fields(self) -> dict:
        if isinstance(self.instance, DirtyFieldsMixin):
            relevant_fields = [field.name for field in self._relevant_model_fields]
            return {field: value for field, value in self.instance.get_dirty_fields(check_relationship=True).items() if field in relevant_fields}

        else:
            warnings.warn("Cannot access get_dirty_fields method. The DirtyFieldsMixin (from django-dirtyfields) is not added to your model.")

        return {}

    @cached_property
    def tenant_id(self) -> Optional[str]:
        return self.instance.tenant_id if self.is_tenant_bound else None

    def check_if_modified(self) -> bool:
        if self.signal == post_save and self.kwargs.get('created'):
            return True

        if isinstance(self.instance, DirtyFieldsMixin):
            return bool(self.modified_fields)

        return True

    def get_body(self) -> DataChangeEvent:
        data = self.get_data()

        if self.is_changed_included:
            data['_changed'] = [
                {
                    'name': field,
                    'previous_value': value,
                } for field, value in self.modified_fields.items()
            ]

        return DataChangeEvent(
            data=data,
            data_type=self.data_type,
            data_op=self.data_op,
            tenant_id=self.tenant_id,
            metadata=self.metadata,
        )

    def get_headers(self):
        return {
            'data_type': self.data_type,
            'data_op': self.data_op.value,
            'record_id': self.instance.id,
            'tenant_id': self.tenant_id,
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
        if span:
            span.set_tag('topic', self.topic)
            span.set_tag('sender', self.sender)
            span.set_tag('signal', self.signal)
            span.set_tag('orm_model', self.orm_model)

        self.logger.debug("Publish DataChangeEvent for %s with schema %s on %r", self.orm_model, self.event_schema, self.topic)
        if not self.is_modified:
            self.logger.debug("Not publishing DataChangeEvent, not modified")
            return

        with start_span(op='KafkaProducer.send'):
            future_message: FutureRecordMetadata = self.connection.send(**self.data)

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
    def _register(cls, model: Type[models.Model] = None, parent_foreignkeys: List[ReverseManyToOneDescriptor] = []):
        save_handler_name = f'_handle_post_save_{model.__qualname__}'
        def handle(sender, instance: TDjangoModel, signal: Signal, **kwargs):
            for foreignkey in parent_foreignkeys:
                instance = getattr(instance, foreignkey.name)

            return cls.handle(sender, instance, signal, **kwargs)

        setattr(cls, save_handler_name, partial(handle, signal=post_save))
        receiver(post_save, sender=model)(getattr(cls, save_handler_name))

        if cls._kwargs.get('is_post_delete_received', True):
            delete_handler_name = f'_handle_post_delete_{model.__qualname__}'
            setattr(cls, delete_handler_name, partial(handle, signal=post_delete))
            receiver(post_delete, sender=model)(getattr(cls, delete_handler_name))

        cls.logger.debug("Registered post_save + post_delete handlers for %s", model)

    @classmethod
    def _register_submodels(cls):
        for reference in cls.submodels:
            model = cls.orm_model
            parent_foreignkeys = []
            for ref in reference.split('.'):
                descriptor: ReverseManyToOneDescriptor = getattr(model, ref)
                parent_foreignkeys.insert(0, descriptor.rel.field)
                model = descriptor.rel.related_model

            cls._register(model, parent_foreignkeys)

    @classmethod
    def register(cls):
        cls._register(cls.orm_model)
        cls._register_submodels()


class StatusChangePublisher(with_typehint(EventPublisher)):
    @property
    def action(self):
        return str(self.instance.status).lower()

    @classmethod
    def register(cls):
        cls._handle_status_change = partial(cls.handle, signal=cls.orm_model.STATUS_CHANGE)
        receiver(cls.orm_model.STATUS_CHANGE, sender=cls.orm_model)(cls._handle_status_change)

        cls.logger.debug("Registered status_change handlers for %s", cls.orm_model)
