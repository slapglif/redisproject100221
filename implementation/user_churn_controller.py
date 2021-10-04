from redis_services import Register, consume_new_group_event, handle_consumer_data
from implementation.user_churn_service import handler_service


@Register(stream="stream-1", group="group-1", consumer="consumer-1")
async def watch_stream(*_, **kwargs):
    """
    :param kwargs:
        stream and group kwargs injected into the function
    :return:
        response data from the stream
    """
    # consume stream data
    consumer_data = await consume_new_group_event(**kwargs)
    response = await handle_consumer_data(consumer_data, **kwargs)

    # send data to callback function
    if response:
        await handler_service(response)
        print(response)

    return response


