from asyncio import Future as AsyncioFuture
from kafka.future import Future as KafkaFuture


def resolve_future_async(future: KafkaFuture):
    _future = AsyncioFuture()
    future.add_callback(_future.set_result)
    future.add_errback(_future.set_exception)

    return _future
