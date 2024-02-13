from nats_contractor.nats import NatsQA
from nats_contractor.nats_streaming import NatsStreamingQA
from log.app import Logger

logger = Logger(service_name='integration-tests-nats-lib', log_level='debug')
logger.log_to_logstash(host="0.0.0.0", port=6004, logstash_network='udp')

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
