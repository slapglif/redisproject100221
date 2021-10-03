from fastapi import FastAPI, Request
from json import JSONDecodeError
import redis_services

app = FastAPI()


@app.post("/api/v1/publish_message")
async def publish_message(request: Request) -> dict:
    try:
        body = await request.json()

        channels: list = body.get("channels")
        if not channels:
            response = {
                "success": False,
                "response": "must provide valid channels list"
            }
            return response

        message: dict = body.get("message")
        if not message:
            response = {
                "success": False,
                "response": "You must provide valid message dictionary"
            }
            return response

        await redis_services.publish(channels, message)
        response = {"success": True}

    except JSONDecodeError:
        response = {
            "response": "Received data is not a valid JSON",
            "success": False
        }
    return response


@app.post("/api/v1/create_stream")
async def create_stream(request: Request):
    try:
        body = await request.json()

        stream_name: str = body.get("stream_name")
        if not stream_name:
            response = {
                "success": False,
                "response": "no stream_name provided"
            }
            return response

        fields: dict = body.get("fields")
        if not fields:
            response = {
                "success": False,
                "response": "Valid field dict not provided"
            }
            return response

        redis_response = await redis_services.create_stream(stream_name, fields)
        response = {'success': True, "response": redis_response}

    except JSONDecodeError:
        response = {
            "response": "Received data is not a valid JSON",
            "success": False
        }
    return response


@app.post("/api/v1/create_group")
async def create_group(request: Request):
    try:
        body = await request.json()

        stream_name: str = body.get("stream_name")
        if not stream_name:
            response = {
                "success": False,
                "response": "no stream_name provided"
            }
            return response

        group_name: str = body.get("group_name")
        if not group_name:
            response = {
                "success": False,
                "response": "no group_name provided"
            }
            return response

        redis_response = await redis_services.create_group(stream_name, group_name)
        return {"success": True, "response": redis_response}

    except JSONDecodeError:
        response = {
            "response": "Received data is not a valid JSON",
            "success": False
        }
    return response


@app.post("/api/v1/get_streams_info")
async def get_stream_info(request: Request):
    try:
        body = await request.json()

        stream_name: str = body.get("stream_name")
        if not stream_name:
            response = {
                "success": False,
                "response": "stream_name not provided"
            }
            return response

        stream_info = await redis_services.stream_info(stream_name)
        response = {"success": True, "response": stream_info}

    except JSONDecodeError:
        response = {
            "response": "Received data is not a valid JSON",
            "success": False
        }
    return response


@app.post("/api/v1/get_groups_info")
async def get_groups_info(request: Request):
    try:
        body = await request.json()

        stream_name: str = body.get("stream_name")
        if not stream_name:
            response = {
                "success": False,
                "response": "stream_name not provided"
            }
            return response

        groups_info = await redis_services.groups_info(stream_name)
        response = {"success": True, "response": groups_info}

    except JSONDecodeError:
        response = {
            "response": "Received data is not a valid JSON",
            "success": False
        }
    return response


@app.post("/api/v1/get_consumers_info")
async def get_consumer_info(request: Request):
    try:
        body = await request.json()

        stream_name: str = body.get("stream_name")
        if not stream_name:
            response = {
                "success": False,
                "response": "stream_name not provided"
            }
            return response

        group_name: str = body.get("group_name")
        if not group_name:
            response = {
                "success": False,
                "response": "no group_name provided"
            }
            return response

        consumers_info = await redis_services.consumers_info(stream_name, group_name)
        response = {"success": True, "response": consumers_info}

    except JSONDecodeError:
        response = {
            "response": "Received data is not a valid JSON",
            "success": False
        }
    return response