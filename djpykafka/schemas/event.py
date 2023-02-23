from enum import Enum
import secrets
from datetime import datetime, timezone as tz
from django.utils import timezone
from django.conf import settings
from typing import Any, List, Optional, Tuple
from pydantic import BaseModel, Field, validator
try:
    import sentry_sdk

except ImportError:
    sentry_sdk = None

from djdantic import context


CURRENT_SOURCE = getattr(settings, 'APP_NAME', settings.SETTINGS_MODULE.split('.', 1)[0])


def default_eid():
    return secrets.token_hex(32)


def _get_flow_id():
    try:
        return sentry_sdk.Hub.current.scope.transaction.to_traceparent()

    except (KeyError, AttributeError, IndexError, TypeError):
        return None


def _get_uid() -> Optional[str]:
    try:
        return context.access.get().user_id

    except LookupError:
        return

def _get_scopes() -> list:
    try:
        return context.access.get().token.aud

    except LookupError:
        return []

def _get_roles() -> list:
    try:
        return context.access.get().token.rls

    except LookupError:
        return []


def _get_user():
    if not _get_uid():
        return

    return EventMetadata.User()


def _get_sources():
    sources = [CURRENT_SOURCE]
    try:
        sources += context.access.get().sources

    except LookupError:
        pass

    return sources


def _get_parent_eids():
    eids = []
    try:
        eids += context.access.get().eids

    except LookupError:
        pass

    return eids


class Version(BaseModel):
    __root__: Tuple[int, int, int]

    def __str__(self):
        return '.'.join([str(v) for v in self.__root__])

    def __repr__(self):
        return f'Version({self})'


class EventMetadata(BaseModel):
    class User(BaseModel):
        uid: Optional[str] = Field(default_factory=_get_uid)
        scopes: List[str] = Field(default_factory=_get_scopes)
        roles: List[str] = Field(default_factory=_get_roles)

    eid: str = Field(min_length=64, max_length=64, default_factory=default_eid)
    event_type: Optional[str]
    occurred_at: datetime = Field(default_factory=timezone.now)
    # received_at
    version: Optional[Version] = Field()
    user: Optional[User] = Field(default_factory=_get_user)
    parent_eids: List[str] = Field(default_factory=_get_parent_eids)
    sources: List[str] = Field(default_factory=_get_sources)
    flow_id: Optional[str] = Field(default_factory=_get_flow_id)

    @validator('occurred_at')
    def force_timezone(cls, value: datetime):
        if not value.tzinfo:
            return value.replace(tzinfo=tz.utc)

        return value


class GeneralEvent(BaseModel):
    metadata: EventMetadata = Field(default_factory=EventMetadata)


class DataChangeEvent(GeneralEvent):
    class DataOperation(Enum):
        CREATE = 'C'
        UPDATE = 'U'
        DELETE = 'D'
        SNAPSHOT = 'S'

    data: Any
    data_type: str
    data_op: DataOperation
    tenant_id: Optional[str]
