"""
test for:
start_listen
send
wait_msgs
total_handle
"""
import asyncio
from google.protobuf.reflection import ParseMessage
from integration_tests.api.simpleMessage_pb2 import SimpleMessage
from integration_tests.src.settings import nats, logger, subjects


def test_base_functions():
    loop = nats.loop = asyncio.get_event_loop()
    nats_resp, request_respond = loop.run_until_complete(_test_send())
    logger.info("nats resp: {}".format(nats_resp))
    assert nats_resp["test_topic2"] == []
    assert ParseMessage(SimpleMessage.DESCRIPTOR, nats_resp["test_topic1"][0]).Body == b'test_proto_1'


async def _test_send():
    test_proto_1 = SimpleMessage()
    test_proto_1.Body = b'test_proto_1'

    await nats.start_listen_all()
    await nats.send(subjects[0], test_proto_1)
    request_respond = None
    nats_resp = await nats.wait_msgs(msgs_await=1)
    return nats_resp, request_respond
