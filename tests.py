import asyncio
import redis_services
from types import SimpleNamespace

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# asyncio.run(subscribe(["channel:1", "channels:2"]))
# asyncio.run(redis.stream_info("potato-1"))

async def test_create_streams():
    await redis_services.create_stream("stream-1", {"test": 1})
    await redis_services.create_stream("stream-2", {"test": 2})
    await redis_services.delete_stream("stream-1")
    await redis_services.delete_stream("stream-2")


async def test_stream_info():
    await redis_services.create_stream("stream-1", {"test": 1})
    await redis_services.stream_info("stream-1")


async def test_destroy_stream():
    await redis_services.create_stream("stream-1", {"test": 1})
    await redis_services.delete_stream("stream-1")


async def test_create_group():
    await redis_services.create_stream("stream-1", {"test": 1})
    await redis_services.create_group("stream-1", "group-1")


async def test_destroy_group():
    await redis_services.create_stream("stream-1", {"test": 1})
    await redis_services.create_group("stream-1", "group-1")
    await redis_services.delete_stream("stream-1")


@redis_services.Register(stream="stream-1", group="group-1")
async def test_wrapper(*args, **kwargs):
    registration = SimpleNamespace(**kwargs)
    consumer_data = await redis_services.consume_new_group_event("consumer-1", **kwargs)
    await redis_services.write_stream(registration.stream, {"new_data": "handle_me_pls"})
    response = await redis_services.handle_consumer_data(consumer_data, **kwargs)
    print(response)

asyncio.run(test_wrapper())

# # create multiple streams
# asyncio.run(test_create_streams())
#
# # test stream info
# asyncio.run(test_stream_info())
#
# # test destroy stream
# asyncio.run(test_destroy_stream())

# # test create group
# asyncio.run(test_create_group())

# test destroy group
# asyncio.run(test_destroy_group()

# test subscribe
# asyncio.run(redis_services.subscribe(["channel:1"]))
