"""
class for work with nats-streaming
"""
import datetime
from nats.aio.client import Client as Nats
from stan.aio.client import Client as Stan
from nats_contractor.nats_base_class import NatsBaseQA


class NatsStreamingQA(NatsBaseQA):

    def __init__(self, logger, subjects, connect_string, nats_timeout=2, add_await=0.1, msgs_await=0,
                 durable_name="durable_name", cluster_name="test-cluster"):
        """
        :param logger: logger class instance
        :param subjects: list of stan topics for subscribe in start_listen_all with _total_handle for all
        :param connect_string: nats uri, example format: nats://0.0.0.0:5644
        :param nats_timeout: global timeout for wait_msgs (used if local timeout don`t set), u can set local timeout directly in function
        :param add_await: simple global wait after receive all planning msgs or timeout (used if local add_await don`t set),
        u can set local add_await directly in function
        :param msgs_await: global total msgs count waiting until timeout in handlers, u can set local msgs_await directly in function
        :param durable_name: global stan subscription durable_name, u can set local durable_name directly in function
        :param cluster_name: stan cluster name, use test-cluster for docker nats-streaming
        """
        NatsBaseQA.__init__(self, logger, subjects, connect_string, nats_timeout, add_await, msgs_await)

        self._sc = None
        self.global_durable_name = durable_name
        self.cluster_name = cluster_name

    async def send(self, topic: str, message):
        """
        nats publish in new nats connection

        :param topic: nats topic for publish
        :param message: protobuf class
        """
        try:
            nc = Nats()
            sc = Stan()
            await nc.connect(io_loop=self._loop, servers=[self.connect_string])
            await sc.connect(self.cluster_name, str(datetime.datetime.utcnow().microsecond), nats=nc)

            self._logger.info("stan {} > {}".format(topic, message))
            await sc.publish(topic, message.SerializeToString())

            await sc.close()
            await nc.close()
        except Exception as e:
            self._logger.error("stan send error: {}".format(e))

    async def start_listen_all(self, durable_name="use global_durable_name"):
        """
        subscribe for list of subjects (passing in init) with _total_handle
        clear msgs counter, coming in handlers msgs dict and ssids list
        create connect to stan (global for class) only one time, connection can be close only by wait_msgs function
        use wait_msgs function for collect all data coming in nats handler
        :param durable_name: stan subscription durable_name, use default for use global durable name from init
        """
        try:
            if durable_name == "use global_durable_name":
                durable_name = self.global_durable_name

            await NatsBaseQA.start_listen_all(self)

            self._sc = Stan()
            await self._sc.connect(self.cluster_name, str(datetime.datetime.utcnow().microsecond), nats=self._nc)

            for subject in self._subjects:
                self._subjects[subject] = []
                ssid = await self._sc.subscribe(subject=subject, cb=self._total_handle,
                                                durable_name=durable_name, error_cb=self._error_handler)
                self.ssids.append(ssid)
        except Exception as e:
            self._logger.error("stan start_listen error: {}".format(e))

    async def wait_msgs(self, msgs_await=None, timeout=None, add_await=None) -> dict:
        """
        1 wait count msgs in all nats handlers
        2 wait add_await time
        3 close all connections, unsubscribe all topics
        4 return dict of all msgs coming in stan handlers
        :param msgs_await: int, wait count msgs in all stan handlers, if not set, used global
        :param timeout: float, seconds, timeout for wait count msgs in all stan handlers, if not set, used global
        :param add_await: float, seconds, wait some more time after receive all msgs_await or timeout, if not set, used global
        :return: _subjects dict, all received msgs, format {"topic_1": [b'received msg 1', b'received msg 2'], ...}
        """
        try:
            await NatsBaseQA.wait_msgs(self, msgs_await, timeout, add_await)

            for ssid in self.ssids:
                await ssid.unsubscribe()

            await self._sc.close()
            await self._nc.close()
            self._loop, self._nc, self._sc = None, None, None

            return self._subjects
        except Exception as e:
            self._logger.error("stan return_msgs error: {}".format(e))

    async def _total_handle(self, msg):
        """
        handler for start_listen_all, collect all msgs in _subjects dict and count them
        :param msg: received msg
        """
        try:
            self._logger.info("stan got message, topic: {}".format(msg.sub.subject))
            self._subjects[msg.sub.subject].append(msg.data)
            self.total_msg += 1
        except Exception as e:
            self._logger.error("stan total_handle error: {}".format(e))

    async def _error_handler(self, msg):
        """
        async callback called on error, with the exception as the sole argument.
        :param msg: received exception msg
        """
        self._logger.error("nats_base error_handler: {}".format(msg))
