# nats_client

#### Installation:

pip install git+ssh://git@gitaddress.com/qa/nats_client.git

env:
asyncio-nats-client
asyncio-nats-streaming

## Description

Module for working with message brokers nats and nats-streaming

## Example of work:
```python
# settings.py
from nats_contractor.nats import NatsQA
from nats_contractor.nats_streaming import NatsStreamingQA

subjects = [
    "test_topic1",
    "test_topic2"
]

nats_connect_string = "nats://0.0.0.0:5644"
stan_connect_string = "nats://0.0.0.0:5544"
global_nats_timeout = 2
add_await = 0.01

nats = NatsQA(logger, subjects, nats_connect_string, nats_timeout=global_nats_timeout, add_await=add_await)
stan = NatsStreamingQA(logger, subjects, stan_connect_string, nats_timeout=global_nats_timeout, add_await=add_await,
                       durable_name="durable_name", cluster_name="test-cluster")

# *.py
import asyncio
from settings import nats

def test_communication_logic():
    loop = nats.loop = asyncio.get_event_loop()
    return loop.run_until_complete(_test_communication_logic())

async def _test_communication_logic():
    await nats.start_listen_all()
    await nats.start_listen_with_respond(topic="test_topic_request_respond", respond_proto=test_proto_respond)
    await nats.send("topic", test_proto_1)
    def_send_some_in_service()
    request_respond = await nats.request_respond("test_topic_request_respond", test_proto_request)
    nats_resp = await nats.wait_msgs(msgs_await=2)
    return nats_resp, request_respond

```

## Recommendations for work

It is recommended to initialize the natz and/or natz-streaming object once in the entire project, and then import the object itself.

## Installation and update options

```
To use the project, you need to register an ssh key in the system and git.

Module update: pip install --upgrade git+ssh://git@gitaddress.com/qa/nats_client.git

To install (update) a module from a specific branch: pip install (--upgrade) git+ssh://git@gitaddress.com/qa/nats_client.git@branch_name

To install a specific package use eggs: pip install git+ssh://git@gitaddress.com/qa/nats_client.git@branch_name#egg=logger

```
