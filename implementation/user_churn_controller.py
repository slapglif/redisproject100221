from ..redis_services import Register, consume_new_group_event, handle_consumer_data
from user_churn_service import handler_service


@Register(stream="stream-1", group="group-1")
async def watch_stream(*_, **kwargs):
    """
    :param kwargs:
        stream and group kwargs injected into the function
    :return:
        response data from the stream
    """

    # define consumer name
    consumer_name = "consumer-1"

    # consume stream data
    consumer_data = await consume_new_group_event(consumer_name, **kwargs)
    response = await handle_consumer_data(consumer_data, **kwargs)

    # send data to callback function
    await handler_service(response)

    return response
