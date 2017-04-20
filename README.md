# ESL RabbitMQ Client

Wrapper that works on top of [RabbitMQ Erlang Client](https://github.com/rabbitmq/rabbitmq-erlang-client).

This is also a nice API for using RabbitMQ through Erlang.

This project aims to make RabbitMQ developers' life easier by not having to
know/use all the rabbitmq-erlang-client entities (records). Also it handles
reconnection automatically, so you don't need to worry about it.

## Getting Started

* Connect to RabbitMQ:
```
Add `esl_rabbitmq_client` as a dependency of your application and make sure it is started before your app starts
```

* Declare exchange:
```
esl_rabbitmq_client_worker:create_exchange(Exchange, <<"fanout">>, durable),
```

* Declare queue:
```
Queue = esl_rabbitmq_client_worker:create_queue( [{exclusive, true}] ),
```

* Bind Queue:
```
esl_rabbitmq_client_worker:bind_queue(Queue, Exchange, <<"r-key">>),
```

* Consume messages:
```
esl_rabbitmq_client_worker:consume(Queue, self()),
```

* Publish a message:
```
esl_rabbitmq_client_worker:publish(<<"my-exch">>, <<"r-key">>, <<"my-message">>),
```

* Acknowledge a message:
```
esl_rabbitmq_client_worker:acknowledge_message(DeliveryTag),
```

---

If you want to use different values than the default ones when connecting to a
RabbitMQ node, just set the parameters within your app's configuration file.

Also if you want to set some extra parameters when declaring queues or when
declaring exchanges you can use `esl_rabbitmq_client_worker:create_queue/1` and
`esl_rabbitmq_client_worker:create_exchange/1` respectively. You have the same
options when publishing, so if you need to set some properties of a message you
can use `esl_rabbitmq_client_worker:publish/1`.
