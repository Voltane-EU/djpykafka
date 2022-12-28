import os
from collections import defaultdict
from asyncapi_docgen.constants import REF_PREFIX
from asyncapi_docgen.models import Channel, Operation, Message
from pydantic import create_model, Field
from pydantic.schema import get_model_name_map, get_flat_models_from_models
from fastapi.utils import get_model_definitions
from asyncapi_docgen.utils import get_asyncapi as _get_asyncapi
from django.conf import settings
from ..events.publish import EventPublisher
from ..schemas.event import DataChangeEvent


def get_components():
    channels: defaultdict[str, Channel] = defaultdict(Channel)
    wrapped_event_schemas = {
        publisher.event_schema: create_model(
            f'{publisher.event_schema.__qualname__} [W]',
            __base__=(DataChangeEvent,),
            __module__=publisher.event_schema.__module__,
            data=(publisher.event_schema, ...),
        ) for publisher in EventPublisher.__subclasses__()
    }
    flat_models = get_flat_models_from_models(wrapped_event_schemas.values())
    model_name_map = get_model_name_map(flat_models)
    definitions = get_model_definitions(flat_models=flat_models, model_name_map=model_name_map)
    for publisher in EventPublisher.__subclasses__():
        channels[publisher.topic].publish = Operation(
            message=Message(payload={'$ref': REF_PREFIX + model_name_map[wrapped_event_schemas[publisher.event_schema]]}),
        )

    return {k: definitions[k] for k in sorted(definitions)}, channels


def get_asyncapi():
    schemas, channels = get_components()
    return _get_asyncapi(
        title=getattr(settings, 'APP_NAME') or os.getenv('DJANGO_SETTINGS_MODULE', '').split('.')[0] or 'AsyncAPI',
        version=getattr(settings, 'APP_VERSION') or '0.0.1',
        channels=channels,
        components={
            'schemas': schemas,
        }
    )
