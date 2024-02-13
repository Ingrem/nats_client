import asyncio
from google.protobuf.reflection import ParseMessage
from integration_tests.api.simpleMessage_pb2 import SimpleMessage
from integration_tests.src.settings import nats, logger, subjects


def test_request_respond():
    loop = nats.loop = asyncio.get_event_loop()
    nats_resp, request_respond = loop.run_until_complete(_test_send())
    logger.info("nats resp: {}".format(nats_resp))
    logger.info("nats request_respond: {}".format(request_respond))
    assert ParseMessage(SimpleMessage.DESCRIPTOR, nats_resp["test_topic_request_respond"][0]).Body == b'test_proto_request'
    assert ParseMessage(SimpleMessage.DESCRIPTOR, request_respond).Body == b'test_proto_respond'
    assert ParseMessage(SimpleMessage.DESCRIPTOR, nats_resp["test_topic2"][0]).Body == b'test_proto_1'


async def _test_send():
    test_proto_request = SimpleMessage()
    test_proto_request.Body = b'test_proto_request'

    test_proto_1 = SimpleMessage()
    test_proto_1.Body = b'test_proto_1'

    test_proto_respond = SimpleMessage()
    test_proto_respond.Body = b'test_proto_respond'

    await nats.start_listen_with_respond(topic="test_topic_request_respond", respond_proto=test_proto_respond)
    await nats.start_listen_all()
    await nats.send(subjects[1], test_proto_1)
    request_respond = await nats.request_respond("test_topic_request_respond", test_proto_request)
    nats_resp = await nats.wait_msgs(msgs_await=2)
    return nats_resp, request_respond
