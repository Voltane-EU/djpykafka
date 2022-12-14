from datetime import timedelta
from django.db import models


class KafkaPublishMixin(models.Model):
    KAFKA_PUBLISH_TIMEDELTA = timedelta(seconds=10)

    last_kafka_publish_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    @classmethod
    def objects_kafka_publish_lag(cls):
        return cls.objects.filter(models.Q(last_kafka_publish_at__isnull=True) | models.Q(last_kafka_publish_at__lt=models.F('updated_at') + cls.KAFKA_PUBLISH_TIMEDELTA))

    class Meta:
        abstract = True


class KafkaSubscribeMixin(models.Model):
    updated_at = models.DateTimeField()

    class Meta:
        abstract = True
