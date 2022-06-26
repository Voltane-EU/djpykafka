from django.db import models


class KafkaMixin(models.Model):
    last_kafka_publish_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True
