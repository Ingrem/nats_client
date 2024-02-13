"""
abstract base class for work with nats and nats-streaming
"""
import asyncio
import time
from abc import ABC, abstractmethod
from nats.aio.client import Client as Nats


class NatsBaseQA(ABC):

    def __init__(self, logger, subjects: list, connect_string: str, nats_timeout=2, add_await=0.1, msgs_await=0):
        """
        base init inherited in nats and override in nats-streaming

        :param logger: logger class instance
        :param subjects: list of nats/stan topics for subscribe in start_listen_all with _total_handle for all
        :param connect_string: nats uri, example format: nats://0.0.0.0:5644
        :param nats_timeout: global timeout for wait_msgs (used if local timeout don`t set), u can set local timeout directly in function
        :param add_await: simple global wait after receive all planning msgs or timeout (used if local add_await don`t set),
        u can set local add_await directly in function
        :param msgs_await: global total msgs count waiting until timeout in handlers, u can set local msgs_await directly in function
        """
        self._loop, self._nc = None, None
        self.ssids = []
        self.total_msg = 0

        self.connect_string = connect_string
        self.global_timeout = nats_timeout
        self.global_add_wait = add_await
        self.global_msgs_await = msgs_await
        self._logger = logger

        self.__subjects_list = subjects
        self._subjects = {}
        for subj in subjects:
            self._subjects[subj] = []

    @property
    def loop(self):
        """
        :return: asyncio event_loop
        """
        return self._loop

    @loop.setter
    def loop(self, loop):
        """
        :param loop: asyncio event_loop
        """
        self._loop = loop

    @property
    def subjects(self) -> dict:
        """
        :return: subjects: dict with all msgs received in nats handlers
        """
        return self._subjects

    @subjects.setter
    def subjects(self, subjects: dict):
        """
        use for clear subjects, necessary if subscribe with start_listen_with_respond on different topics
        :param subjects: dict with all msgs received in nats handlers
        """
        self._subjects = subjects

    @abstractmethod
    async def send(self, topic: str, message):
        """
        nats/stan publish in new nats/stan connection

        :param topic: nats/stan topic for publish
        :param message: protobuf class
        """

    @abstractmethod
    async def start_listen_all(self):
        """
        subscribe for list of subjects (passing in init) with _total_handle
        clear msgs counter, coming in handlers msgs dict and ssids list
        create connect to nats (global for class) only one time, connection can be close only by wait_msgs function
        use wait_msgs function for collect all data coming in nats handler
        """
        self.ssids = []
        self.total_msg = 0
        self._subjects = {}
        for subj in self.__subjects_list:
            self._subjects[subj] = []

        if not self._nc:
            self._nc = Nats()
            await self._nc.connect(io_loop=self._loop, servers=[self.connect_string])

    @abstractmethod
    async def wait_msgs(self, msgs_await=None, timeout=None, add_await=None):
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
        if not self._nc:
            raise UnboundLocalError("nats connection don`t exist")

        if not msgs_await:
            msgs_await = self.global_msgs_await
        if not timeout:
            timeout = self.global_timeout
        if not add_await:
            add_await = self.global_add_wait

        start_time = time.time()
        while self.total_msg < msgs_await:
            if time.time() - start_time > float(timeout):
                self._logger.error("nats_base wait_msgs timeout")
                break
            await asyncio.sleep(0.001, loop=self._loop)

        await asyncio.sleep(add_await, loop=self._loop)

    @abstractmethod
    async def _total_handle(self, msg):
        """
        handler for start_listen_all, collect all msgs in _subjects dict and count them
        :param msg: received msg
        """
