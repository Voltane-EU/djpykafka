import warnings
import logging
import json
import re
from typing import Any, Type, TypeVar, Optional, Union
from datetime import datetime
from pydantic import BaseModel
from django.db import models
from django.db.transaction import atomic
from kafka.consumer.fetcher import ConsumerRecord
from sentry_tools import set_extra
from sentry_tools.span import set_tag
from sentry_tools.decorators import instrument_span
from djdantic.utils.pydantic_django import transfer_to_orm, TransferAction
from djdantic.utils.pydantic_django.pydantic import get_sync_matching_filter, get_sync_matching_values
from djdantic.schemas import Access, AccessToken
from djdantic import context
from ..handlers.event_consumer import message_handler
from ..schemas.event import DataChangeEvent
from ..models import KafkaSubscribeMixin


try:
    from dirtyfields import DirtyFieldsMixin

except ImportError:
    class DirtyFieldsMixin:
        pass


TBaseModel = TypeVar('TBaseModel', bound=BaseModel)
TDjangoModel = TypeVar('TDjangoModel', bound=models.Model)


DATA_OPERATIONS = {o.value: o for o in DataChangeEvent.DataOperation}


class Break(Exception):
    pass


class BaseSubscription:
    logger: logging.Logger
    event_schema: Type[TBaseModel]
    topic: str
    topic_use_regex: bool = False

    @classmethod
    def _init_class(
        cls,
        event_schema: Type[TBaseModel],
        topic: str,
        topic_use_regex: bool = False,
    ):
        cls.event_schema = event_schema
        cls.topic = topic
        cls.topic_use_regex = topic_use_regex
        cls.logger = logging.getLogger(f'{cls.__module__}.{cls.__qualname__}')

        message_handler(
            topic=cls.topic,
            is_topic_regex=cls.topic_use_regex,
            transaction_name=f'{cls.__module__}.{cls.__qualname__}',
        )(cls.handle)

        cls.logger.info(
            "Registered Subscription %r with event_schema %r on topic %s",
            cls,
            cls.event_schema,
            cls.topic,
        )

    @classmethod
    @instrument_span(
        op='EventSubscription',
        description=lambda cls, body, *args, **kwargs: f'{cls}',
    )
    @atomic
    def handle(cls, message: ConsumerRecord):
        instance = cls(message)
        if not instance.do_processing:
            return

        instance.process()

    def parse_body(self) -> Union[str, bytes, dict, Any]:
        return str(self.message.value, 'utf-8')

    def parse_event(self) -> DataChangeEvent:
        return DataChangeEvent.parse_raw(self.body) if isinstance(self.body, (bytes, str)) else DataChangeEvent.parse_obj(self.body)

    def parse_data(self) -> TBaseModel:
        if self.event.data_op == DataChangeEvent.DataOperation.DELETE and not self.event.data:
            return 
            
        if isinstance(self.event.data, (bytes, str)):
            return self.event_schema.parse_raw(self.event.data)

        else:
            return self.event_schema.parse_obj(self.event.data)

    def __init__(self, message: ConsumerRecord):
        self.message = message
        self.body = self.parse_body()
        self.event = self.parse_event()
        self.data: Optional[TBaseModel] = self.parse_data()

        self.logger.info(
            "%s %s eid=%s id=%s flow_id=%s sources=%s",
            self.event.data_op.name,
            self.event.data_type,
            self.event.metadata.eid,
            getattr(self.data, 'id', None),
            self.event.metadata.flow_id,
            self.event.metadata.sources,
        )

        set_tag('topic', self.topic)
        set_tag('data_op', self.event.data_op)
        set_extra('body', self.body)

    @property
    def do_processing(self) -> bool:
        return True

    def process(self):
        raise NotImplementedError


class GenericSubscription(BaseSubscription):
    def __init_subclass__(
        cls,
        event_schema: Type[TBaseModel],
        topic: str,
        topic_use_regex: bool = False,
        **kwargs,
    ):
        super().__init_subclass__(**kwargs)
        cls._init_class(
            event_schema,
            topic,
            topic_use_regex,
        )


class DebeziumSubscription(BaseSubscription):
    def __init_subclass__(
        cls,
        event_schema: Type[TBaseModel],
        topic: str,
        topic_use_regex: bool = False,
        **kwargs,
    ):
        super().__init_subclass__(**kwargs)
        cls._init_class(
            event_schema,
            topic,
            topic_use_regex,
        )

    def parse_event(self) -> DataChangeEvent:
        if self.body == 'null':  # Debezium Tombstone events
            return DataChangeEvent(
                data_op=DataChangeEvent.DataOperation.DELETE,
                data_type='debezium_tombstone',
            )

        return super().parse_event()

    def parse_body(self):
        contents = json.loads(super().parse_body())

        if not contents:
            event_type = 'debezium_tombstone'
            data = None
            operation = DataChangeEvent.DataOperation.DELETE
            timestamp = datetime.now()

        else:
            data = contents.get('data')
            event_type = 'debezium'
            operation = DATA_OPERATIONS.get(
                contents['op'].upper(),
                DataChangeEvent.DataOperation.SNAPSHOT,
            )
            timestamp = datetime.fromtimestamp(contents['ts_ms'] / 1000)

        return {
            'data': data,
            'data_type': 'res.partner',
            'data_op': operation,
            'tenant_id': re.search(self.topic, self.message.topic).group('tenant_id'),
            'metadata': {
                'version': (1, 0, 0),
                'event_type': event_type,
                'occurred_at': timestamp,
                'user': None,
                'sources': [],
                'parent_eids': [],
            }
        }


class EventSubscription(BaseSubscription):
    __orm_obj: Optional[TDjangoModel] = None
    orm_model: Type[TDjangoModel]
    delete_on_status: Optional[Any]
    is_tenant_bound: bool
    create_only_on_op_create: bool

    def __init_subclass__(
        cls,
        event_schema: Type[TBaseModel],
        topic: str,
        orm_model: Type[TDjangoModel],
        delete_on_status: Optional[Any] = None,
        create_only_on_op_create: bool = False,
        **kwargs,
    ):
        super().__init_subclass__(**kwargs)
        cls._init_class(
            event_schema,
            topic,
            orm_model,
            delete_on_status,
            create_only_on_op_create,
        )

    @classmethod
    def _init_class(
        cls,
        event_schema: Type[TBaseModel],
        topic: str,
        orm_model: Type[TDjangoModel],
        delete_on_status: Optional[Any] = None,
        create_only_on_op_create: bool = False,
    ):
        super()._init_class(
            event_schema,
            topic,
        )
        cls.orm_model = orm_model
        cls.delete_on_status = delete_on_status
        cls.create_only_on_op_create = create_only_on_op_create

        if not hasattr(cls.orm_model, 'updated_at'):
            warnings.warn("%s has no field 'updated_at'" % cls.orm_model)

        if not isinstance(cls.orm_model, KafkaSubscribeMixin):
            warnings.warn("Using EventSubscription with a model that doesnt has the KafkaSubscribeMixin is not recommended")

        cls.is_tenant_bound = hasattr(cls.orm_model, 'tenant_id')

    def __init__(self, body):
        super().__init__(body)

        if self.event.metadata.user and self.event.metadata.user.uid:
            context.access.set(Access(
                token=AccessToken(
                    iss='int',
                    iat=0,
                    nbf=0,
                    exp=0,
                    sub=self.event.metadata.user.uid,
                    ten=self.event.tenant_id,
                    aud=self.event.metadata.user.scopes,
                    rls=self.event.metadata.user.roles,
                    jti='int',
                    crt=False,
                ),
                sources=self.event.metadata.sources,
                eids=[*self.event.metadata.parent_eids, self.event.metadata.eid],
            ))

        self.is_new_orm_obj = False
        set_tag('orm_model', self.orm_model)

    def process(self):
        if self.event.data_op == DataChangeEvent.DataOperation.DELETE:
            self.op_delete()

        else:
            self.op_create_or_update()

    def _get_orm_obj(self):
        query = get_sync_matching_filter(self.data)
        if self.is_tenant_bound:
            query &= models.Q(tenant_id=self.event.tenant_id)

        return self.orm_model.objects.get(query)

    @property
    def orm_obj(self) -> TDjangoModel:
        if not self.__orm_obj:
            self.__orm_obj = self._get_orm_obj()
            set_extra('orm_obj', self.__orm_obj)

        return self.__orm_obj

    @orm_obj.setter
    def orm_obj(self, value):
        self.__orm_obj = value
        set_extra('orm_obj', self.__orm_obj)

    def _get_create_data(self):
        fields = {field.field.name: value for field, value in get_sync_matching_values(self.data)} or {'id': self.data.id}
        if self.is_tenant_bound:
            fields['tenant_id'] = self.event.tenant_id

        return fields

    def create_orm_obj(self):
        self.orm_obj = self.orm_model(**self._get_create_data())
        self.is_new_orm_obj = True

    def op_delete(self):
        if self.is_new_orm_obj:
            return

        try:
            self.orm_obj.delete()

        except self.orm_model.DoesNotExist:
            pass

        self.orm_obj = None

    def before_transfer(self):
        if self.delete_on_status and self.data.status == self.delete_on_status:
            self.op_delete()
            raise Break

    def after_transfer(self):
        pass

    def op_create_or_update(self):
        try:
            try:
                if self.orm_obj.updated_at > self.event.metadata.occurred_at:
                    self.logger.warning("Received data older than last record update. Discarding change!")
                    return

            except AttributeError:
                pass

        except self.orm_model.DoesNotExist:
            if self.create_only_on_op_create and self.event.data_op != DataChangeEvent.DataOperation.CREATE:
                self.logger.warning("Received object to update which does not exist. Discarding change!")
                return

            self.create_orm_obj()

        try:
            self.before_transfer()

        except Break:
            return

        try:
            self.orm_obj.updated_at = self.event.metadata.occurred_at

        except AttributeError:
            pass

        transfer_to_orm(self.data, self.orm_obj, action=TransferAction.SYNC, do_not_save_if_no_change=issubclass(self.orm_model, DirtyFieldsMixin))

        self.after_transfer()
