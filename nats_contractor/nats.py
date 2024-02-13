"""
class for work with nats
"""
from nats.aio.errors import ErrTimeout
from nats.aio.client import Client as Nats
from nats_contractor.nats_base_class import NatsBaseQA


class NatsQA(NatsBaseQA):

    async def send(self, topic, message):
        """
        nats publish in new nats connection

        :param topic: nats topic for publish
        :param message: protobuf class
        """
        try:
            nc = Nats()
            await nc.connect(io_loop=self._loop, servers=[self.connect_string])
            self._logger.info("nats {} > {}".format(topic, message))
            await nc.publish(topic, message.SerializeToString())
            await nc.close()
        except Exception as e:
            self._logger.error("nats send error: {}".format(e))

    async def start_listen_all(self):
        """
        subscribe for list of subjects (passing in init) with _total_handle
        clear msgs counter, coming in handlers msgs dict and ssids list
        create connect to nats (global for class) only one time, connection can be close only by wait_msgs function
        use wait_msgs function for collect all data coming in nats handler
        """
        try:
            await NatsBaseQA.start_listen_all(self)

            for subject in self._subjects:
                self._subjects[subject] = []
                ssid = await self._nc.subscribe(subject=subject, cb=self._total_handle)
                self.ssids.append(ssid)
        except Exception as e:
            self._logger.error("nats start_listen error: {}".format(e))

    async def start_listen_with_respond(self, topic: str, respond_proto):
        """
        subscribe for nats topic respond_sub
        clear this topic _subjects
        use wait_msgs function for collect all data coming in nats handler
        :param topic: topic for subscribe
        :param respond_proto: protobuf for handler response
        """
        try:
            if not self._nc:
                self._nc = Nats()
                await self._nc.connect(io_loop=self._loop, servers=[self.connect_string])

            async def respond_handler(msg):
                try:
                    self._logger.info("nats got message, topic: {}".format(msg.subject))
                    if msg.subject not in self._subjects:
                        self._subjects[topic] = []
                    self._subjects[msg.subject].append(msg.data)
                    self.total_msg += 1
                    await self._nc.publish(msg.reply, respond_proto.SerializeToString())
                except Exception as ex:
                    self._logger.error("nats respond_handler error: {}".format(ex))

            self._subjects[topic] = []
            ssid = await self._nc.subscribe(subject=topic, cb=respond_handler)
            self.ssids.append(ssid)
        except Exception as e:
            self._logger.error("nats start_listen error: {}".format(e))

    async def wait_msgs(self, msgs_await=None, timeout=None, add_await=None) -> dict:
        """
        1 wait count msgs in all nats handlers (from subjects and all running start_listen_with_respond topics)
        2 wait add_await time
        3 close all connections, unsubscribe all topics
        4 return dict of all msgs coming in nats handlers
        :param msgs_await: int, wait count msgs in all nats handlers, if not set, used global
        :param timeout: float, seconds, timeout for wait count msgs in all nats handlers, if not set, used global
        :param add_await: float, seconds, wait some more time after receive all msgs_await or timeout, if not set, used global
        :return: _subjects dict, all received msgs, format {"topic_1": [b'received msg 1', b'received msg 2'], ...}
        """
        try:
            await NatsBaseQA.wait_msgs(self, msgs_await, timeout, add_await)
            for ssid in self.ssids:
                await self._nc.unsubscribe(ssid)

            await self._nc.close()
            self._loop, self._nc = None, None
            return self._subjects
        except Exception as e:
            self._logger.error("nats return_msgs error: {}".format(e))

    async def _total_handle(self, msg):
        """
        handler for start_listen_all, collect all msgs in _subjects dict and count them
        :param msg: received msg
        """
        try:
            self._logger.info("nats got message, topic: {}".format(msg.subject))
            self._subjects[msg.subject].append(msg.data)
            self.total_msg += 1
        except Exception as e:
            self._logger.error("nats total_handle error: {}".format(e))

    async def request_respond(self, topic: str, message, timeout=None) -> bytes:
        """
        nats request-respond in new nats connection

        :param topic: nats topic for publish
        :param message: protobuf class
        :param timeout: float, seconds, timeout for wait count msgs in all nats handlers, if not set, used global
        """
        try:
            if not timeout:
                timeout = self.global_timeout

            nc = Nats()
            await nc.connect(io_loop=self._loop, servers=[self.connect_string])

            try:
                self._logger.info("nats {} > {}".format(topic, message))
                resp = await nc.timed_request(topic, message.SerializeToString(), timeout)
                response = resp.data
                self._logger.info("nats {} response < {}".format(topic, response))
            except ErrTimeout:
                response = TimeoutError
                self._logger.error("nats request_respond timeout")

            await nc.close()
            return response
        except Exception as e:
            self._logger.error("nats request_respond error: {}".format(e))
