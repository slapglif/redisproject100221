import uvicorn
from implementation.user_churn_controller import watch_stream
import asyncio
import time
from threading import Thread


def loop_in_thread(loop):
    while True:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(watch_stream())
        time.sleep(1)


loop = asyncio.get_event_loop()
thread = Thread(target=loop_in_thread, args=(loop,))
thread.start()

if __name__ == '__main__':
    uvicorn.run("app.main:app",
                host="127.0.0.1",
                port=8000,
                reload=True,
                )


