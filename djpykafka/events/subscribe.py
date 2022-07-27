import warnings
import logging
import json
from typing import Any, Type, TypeVar, Optional
from pydantic import BaseModel
from django.db import models
from djfapi.utils.pydantic_django import transfer_to_orm, TransferAction
from djfapi.utils.sentry import instrument_span
from djfapi.security.jwt import access as access_ctx
from djfapi.schemas import Access, AccessToken
from ..handlers.event_consumer import message_handler
from ..schemas import DataChangeEvent
try:
    from sentry_sdk import set_extra, Hub

except ImportError:
    def set_extra(key, data):
        pass

    Hub = None


TBaseModel = TypeVar('TBaseModel', bound=BaseModel)
TDjangoModel = TypeVar('TDjangoModel', bound=models.Model)


class Break(Exception):
    pass


class EventSubscription:
    __orm_obj: Optional[TDjangoModel] = None

    def __init_subclass__(
        cls,
        event_schema: Type[TBaseModel],
        orm_model: Type[TDjangoModel],
        topic: str,
        delete_on_status: Optional[Any] = None,
        create_only_on_op_create: bool = False,
        **kwargs,
    ):
        super().__init_subclass__(**kwargs)
        cls.event_schema = event_schema
        cls.orm_model = orm_model
        cls.topic = topic
        cls.delete_on_status = delete_on_status
        cls.create_only_on_op_create = create_only_on_op_create
        cls.logger = logging.getLogger(__name__)

        message_handler(
            topic=cls.topic,
            transaction_name=f'{cls.__module__}.{cls.__qualname__}',
        )(cls.handle)

        if not hasattr(cls.orm_model, 'updated_at'):
            warnings.warn("%s has no field 'updated_at'" % cls.orm_model)

        cls.is_tenant_bound = hasattr(cls.orm_model, 'tenant_id')

        cls.logger.info(
            "Registered EventSubscription %r with event_schema %r and orm_model %s on topic %s",
            cls,
            cls.event_schema,
            cls.orm_model,
            cls.topic,
        )

    @classmethod
    @instrument_span(
        op='EventSubscription',
        description=lambda cls, body, *args, **kwargs: f'{cls}',
    )
    def handle(cls, body):
        instance = cls(body)
        instance.process()

    def __init__(self, body):
        self.body = body
        self.event = DataChangeEvent.parse_raw(self.body) if isinstance(self.body, (bytes, str)) else DataChangeEvent.parse_obj(self.body)

        self.logger.info(
            "%s %s eid=%s id=%s flow_id=%s",
            self.event.data_op.name,
            self.event.data_type,
            self.event.metadata.eid,
            self.event.data.get('id'),
            self.event.metadata.flow_id,
        )

        if self.event.metadata.user and self.event.metadata.user.uid:
            access_ctx.set(Access(
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
            ))

        self.is_new_orm_obj = False
        if Hub:
            self.span = Hub.current.scope.span
            self.span.set_tag('topic', self.topic)
            self.span.set_tag('orm_model', self.orm_model)
            self.span.set_tag('data_op', self.event.data_op)
            set_extra('body', self.body)

    def process(self):
        if self.event.data_op == DataChangeEvent.DataOperation.DELETE:
            self.op_delete()

        else:
            self.op_create_or_update()

    @property
    def orm_obj(self) -> TDjangoModel:
        if not self.__orm_obj:
            data = json.loads(self.body) if isinstance(self.body, (bytes, str)) else self.body
            query = models.Q(id=data.get('id'))
            if self.is_tenant_bound:
                query &= models.Q(tenant_id=self.event.tenant_id)

            self.__orm_obj = self.orm_model.objects.get(query)
            set_extra('orm_obj', self.__orm_obj)

        return self.__orm_obj

    @orm_obj.setter
    def orm_obj(self, value):
        self.__orm_obj = value
        set_extra('orm_obj', self.__orm_obj)

    def create_orm_obj(self, data: TBaseModel):
        fields = {'id': data.id}
        if self.is_tenant_bound:
            fields['tenant_id'] = self.event.tenant_id

        self.orm_obj = self.orm_model(**fields)
        self.is_new_orm_obj = True

    def op_delete(self):
        if self.is_new_orm_obj:
            return

        try:
            self.orm_obj.delete()

        except self.orm_model.DoesNotExist:
            pass

    def before_transfer(self):
        if self.delete_on_status and self.data.status == self.delete_on_status:
            self.op_delete()
            raise Break

    def after_transfer(self):
        pass

    def op_create_or_update(self):
        self.data: TBaseModel = self.event_schema.parse_obj(self.event.data)
        try:
            try:
                if self.orm_obj.updated_at > self.event.metadata.occurred_at:
                    self.logger.warning("Received data older than last record update. Discarding change!", stack_info=True)
                    return

            except AttributeError:
                pass

        except self.orm_model.DoesNotExist:
            if self.create_only_on_op_create and self.event.data_op != DataChangeEvent.DataOperation.CREATE:
                self.logger.warning("Received object to update which does not exist. Discarding change!", stack_info=True)
                return

            self.create_orm_obj(self.data)

        try:
            self.before_transfer()

        except Break:
            return

        try:
            self.orm_obj.updated_at = self.event.metadata.occurred_at

        except AttributeError:
            pass

        transfer_to_orm(self.data, self.orm_obj, action=TransferAction.SYNC)

        self.after_transfer()
