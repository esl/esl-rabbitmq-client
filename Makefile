PROJECT = esl_rabbitmq_client

DEPS = rabbit_common amqp_client

dep_rabbit_common = git https://github.com/rabbitmq/rabbitmq-common rabbitmq_v3_6_5
dep_amqp_client = git https://github.com/rabbitmq/rabbitmq-erlang-client rabbitmq_v3_6_5

include erlang.mk

SHELL_OPTS = -boot start_sasl -s esl_rabbitmq_client
