import aioredis
import asyncio
import async_timeout
from aioredis import exceptions
from config import Settings

redis = aioredis.from_url(
        Settings.global_redis_host, encoding="utf-8", decode_responses=True
    )


async def create_stream(name: str, fields: dict) -> str:
    exists = await redis.exists(name)
    if exists:
        return "Stream already exists"
    if not exists:
        await redis.xadd(name, fields)
        return f"Stream {name} created with fields {fields}"
    await redis.close()


async def stream_info(stream: str) -> dict:
    response = await redis.xinfo_stream(stream)
    await redis.close()
    return response


async def delete_msg(stream: str, ids: list) -> str:
    await redis.xdel(stream, ids)
    await redis.close()
    return f"msg id {id} on stream {stream} deleted"


async def delete_stream(name: str) -> str:
    await redis.delete(name)
    await redis.close()
    return f"Stream {name} destroyed"


async def create_group(stream: str, group: str) -> str:
    try:
        await redis.xgroup_create(stream, group)
        response = f"Group created on {stream} with name {group}"
    except exceptions.ResponseError as e:
        response = str(e)
    await redis.close()
    return response


async def groups_info(stream: str):
    response = await redis.xinfo_groups(stream)
    await redis.close()
    return response


async def delete_group(stream: str, group: str):
    await redis.xgroup_destroy(stream, group)
    return f"Group {group} deleted on stream {stream}"


async def consumers_info(stream: str, group: str) :
    response = await redis.xinfo_consumers(stream, group)
    return response


async def delete_consumer(stream: str, group: str, consumer: str):
    await redis.xgroup_delconsumer(stream, group, consumer)
    return f"Consumer {consumer} deleted on group {group}"


async def reader(channel: aioredis.client.PubSub):
    # TODO: Clarify logic to process the msg data with client
    while True:
        try:
            async with async_timeout.timeout(1):
                message = await channel.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    print(f"(Reader) Message Received: {message}")
                    if message["data"] == "STOP":
                        print("(Reader) STOP")
                        break
                await asyncio.sleep(0.01)
        except asyncio.TimeoutError:
            pass


async def subscribe(channels: list):
    pubsub = redis.pubsub()
    await pubsub.subscribe(*channels)
    asyncio.create_task(reader(pubsub))


async def publish(channels: list, message: dict):
    await redis.publish(*channels, message)



