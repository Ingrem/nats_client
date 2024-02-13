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
from integration_tests.src.settings import stan, logger, subjects


def test_base_functions():
    loop = stan.loop = asyncio.get_event_loop()
    stan_resp = loop.run_until_complete(_test_send())
    logger.info("stan resp: {}".format(stan_resp))
    assert stan_resp["test_topic2"] == []
    assert ParseMessage(SimpleMessage.DESCRIPTOR, stan_resp["test_topic1"][0]).Body == b'test_proto_1'


async def _test_send():
    test_proto_1 = SimpleMessage()
    test_proto_1.Body = b'test_proto_1'

    await stan.start_listen_all()
    await stan.send(subjects[0], test_proto_1)
    stan_resp = await stan.wait_msgs(msgs_await=1)
    return stan_resp
