from typing import Optional, TYPE_CHECKING
from datetime import timedelta, datetime
from django.db import models
from djdantic import BaseModel
from kafka import KafkaProducer

if TYPE_CHECKING:
    from ..events.publish import EventPublisher


class KafkaPublishMixin(models.Model):
    _kafka_publish_topic: Optional[str]
    _kafka_publish_schema: Optional[BaseModel]
    _kafka_publisher: Optional['EventPublisher']
    _kafka_connection: Optional[KafkaProducer]
    KAFKA_PUBLISH_TIMEDELTA = timedelta(seconds=10)

    last_kafka_publish_at: datetime = models.DateTimeField(null=True, blank=True)
    updated_at: datetime = models.DateTimeField(auto_now=True)

    @classmethod
    def _init_publisher(cls):
        from ..events.publish import EventPublisher, DataChangePublisher
        cls._kafka_publisher = type(
            f'{cls.__name__}Publisher',
            (DataChangePublisher, EventPublisher,),
            {},
            orm_model=cls,
            event_schema=cls._kafka_publish_schema,
            topic=cls._kafka_publish_topic,
            data_type=cls._kafka_data_type,
            connection=cls._kafka_connection,
        )

    @classmethod
    def _schemas_set(cls):
        if cls._kafka_publish_topic:
            cls._kafka_publish_schema = cls._schema_response
            cls._init_publisher()

    @classmethod
    def objects_kafka_publish_lag(cls):
        return cls.objects.filter(models.Q(last_kafka_publish_at__isnull=True) | models.Q(last_kafka_publish_at__lt=models.F('updated_at') + cls.KAFKA_PUBLISH_TIMEDELTA))

    def __init_subclass__(
        cls,
        kafka_topic: Optional[str] = None,
        kafka_schema: Optional[BaseModel] = None,
        kafka_data_type: Optional[str] = None,
        kafka_connection: Optional[KafkaProducer] = None,
        **kwargs,
    ):
        super().__init_subclass__(**kwargs)
        cls._kafka_publish_topic = kafka_topic
        cls._kafka_publish_schema = kafka_schema
        cls._kafka_data_type = kafka_data_type or cls.__name__
        cls._kafka_connection = kafka_connection
        if kafka_schema:
            cls._init_publisher()

    class Meta:
        abstract = True


class KafkaSubscribeMixin(models.Model):
    updated_at: datetime = models.DateTimeField()

    class Meta:
        abstract = True
