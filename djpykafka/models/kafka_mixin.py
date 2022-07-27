from django.db import models


class KafkaPublishMixin(models.Model):
    last_kafka_publish_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        abstract = True


class KafkaSubscribeMixin(models.Model):
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True
