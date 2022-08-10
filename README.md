# djpykafka

## 🚧 This project is WIP and is subject to change at any time

This project is currently in the alpha state, even though it can be used in production with some caution. Make sure to fix the version in your requirements.txt and review changes frequently.

## Installation

`pip install djpykafka`

## Examples

Add the `KafkaPublishMixin` to your model class.

```python
from django.db import models
from djpykafka.models import KafkaPublishMixin


class MyModel(KafkaPublishMixin, models.Model):
    ...
```

Create a publisher class, e.g. at `events/publish/{modelname_plural}.py`

```python
from djpykafka.events.publish import EventPublisher, DataChangePublisher
from ... import models
from ...schemas import response
from . import connection


class MyModelPublisher(
    DataChangePublisher,
    EventPublisher,
    orm_model=models.MyModel,
    event_schema=response.MyModel,
    connection=connection,
    topic='bizberry.access.users',
    data_type='access.user',
):
    pass
```
