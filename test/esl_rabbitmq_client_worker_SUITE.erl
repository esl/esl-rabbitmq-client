-module(esl_rabbitmq_client_worker_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").

%% `Common Test' callbacks
-export([ all/0
        , init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        ]).
%% Tests
-export([ create_and_delete_exchange/1
        , create_delete_and_purge_queue/1
        , bind_and_unbind_queue/1
        , publish/1
        , consume/1
        ]).

-type config()   :: term().
-type resource() :: #resource{}.

%% =============================================================================
%% Common Test
%% =============================================================================
-spec all() ->
  [atom()].
all() ->
  [ create_and_delete_exchange
  , create_delete_and_purge_queue
  , bind_and_unbind_queue
  , publish
  , consume
  ].


-spec init_per_suite(Config::config()) ->
  config().
init_per_suite(Config) ->
  rabbit_ct_helpers:log_environment(),
  Config1 = rabbit_ct_helpers:set_config( Config
                                        , [ {rmq_nodename_suffix, ?MODULE}
                                          ]
                                        ),
  Config2 =
    rabbit_ct_helpers:run_setup_steps( Config1
                                     , rabbit_ct_broker_helpers:setup_steps()
                                     ),
  application:set_env(esl_rabbitmq_client, port, 21000),
  {ok, _} = application:ensure_all_started(esl_rabbitmq_client),

  % Make sure testing objects are clean before starting tests
  _ = esl_rabbitmq_client_worker:delete_queue(test_queue()),
  esl_rabbitmq_client_worker:delete_exchange(test_exchange()),

  Config2.


-spec end_per_suite(Config::config()) ->
  config().
end_per_suite(Config) ->
  ok = application:stop(esl_rabbitmq_client),
  rabbit_ct_helpers:run_teardown_steps(
    Config, rabbit_ct_broker_helpers:teardown_steps()).


-spec init_per_testcase(TestCase::atom(), Config::config()) ->
  config().
init_per_testcase(_TestCase, Config) ->
  esl_rabbitmq_client_worker:delete_exchange(test_exchange()),
  Config.


-spec end_per_testcase(TestCase::atom(), Config::config()) ->
  config().
end_per_testcase(_TestCase, Config) ->
  Config.

%% =============================================================================
%% Tests
%% =============================================================================
-spec create_and_delete_exchange(Config::config()) ->
  {comment, string()}.
create_and_delete_exchange(Config) ->
  Exchange = test_exchange(),

  ct:comment("Create exchange with default values but the name"),
  esl_rabbitmq_client_worker:create_exchange(Exchange),
  true = exchange_exist(Config, Exchange),

  ct:comment("Delete exchange with default values but the name"),
  esl_rabbitmq_client_worker:delete_exchange(Exchange),
  false = exchange_exist(Config, Exchange),

  ct:comment("Create durable exchange"),
  esl_rabbitmq_client_worker:create_exchange(Exchange , <<"fanout">> , durable),
  true = exchange_exist(Config, Exchange),

  ct:comment("Delete durable exchange"),
  esl_rabbitmq_client_worker:delete_exchange(Exchange),
  false = exchange_exist(Config, Exchange),

  ct:comment("Create non-durable exchange"),
  esl_rabbitmq_client_worker:create_exchange(Exchange, <<"fanout">>, transient),
  true = exchange_exist(Config, Exchange),

  ct:comment("Delete non-durable exchange"),
  esl_rabbitmq_client_worker:delete_exchange(Exchange),
  false = exchange_exist(Config, Exchange),

  ct:comment("Create non-durable exchange 2"),
  esl_rabbitmq_client_worker:create_exchange(Exchange, <<"fanout">>),
  true = exchange_exist(Config, Exchange),

  ct:comment("Re-create non-durable exchange 2 with type <<\"direct\">>"),
  try
    esl_rabbitmq_client_worker:create_exchange(Exchange, <<"direct">>)
  catch
    exit:{{{shutdown, {_, 406, ErrorMsg}}, _}, _} ->
      ct:pal("Exchange redeclaration error: ~p", [ErrorMsg])
  end,

  ct:comment("Delete non-durable exchange 2"),
  esl_rabbitmq_client_worker:delete_exchange(Exchange),
  false = exchange_exist(Config, Exchange),

  {comment, ""}.


-spec create_delete_and_purge_queue(Config::config()) ->
  {comment, string()}.
create_delete_and_purge_queue(Config) ->
  ct:comment("Create non-durable queue with random name"),
  RandomQueue = esl_rabbitmq_client_worker:create_queue(),
  true = queue_exist(Config, RandomQueue),

  ct:comment("Delete non-durable queue with random name"),
  0 = esl_rabbitmq_client_worker:delete_queue(RandomQueue),
  false = queue_exist(Config, RandomQueue),

  ct:comment("Create non-durable queue with given name"),
  TestQueue = test_queue(),
  TestQueue = esl_rabbitmq_client_worker:create_queue(TestQueue),
  true = queue_exist(Config, TestQueue),

  ct:comment("Delete non-durable queue with given name"),
  0 = esl_rabbitmq_client_worker:delete_queue(TestQueue),
  false = queue_exist(Config, TestQueue),

  ct:comment("Create durable queue with given name"),
  TestQueue = esl_rabbitmq_client_worker:create_queue(TestQueue, durable),
  true = queue_exist(Config, TestQueue),

  ct:comment("Re-create durable queue with given name and make it non-durable"),
  try
    TestQueue = esl_rabbitmq_client_worker:create_queue(TestQueue, transient)
  catch
    exit:{{{shutdown, {_, 406, ErrorMsg}}, _}, _} ->
      ct:pal("Queue redeclaration error: ~p", [ErrorMsg])
  end,

  ct:comment("Delete durable queue with given name"),
  0 = esl_rabbitmq_client_worker:delete_queue(TestQueue),
  false = queue_exist(Config, TestQueue),

  ct:comment("Create queue to purge"),
  PurgeQueue = esl_rabbitmq_client_worker:create_queue(),
  true = queue_exist(Config, PurgeQueue),

  ct:comment("Fill queue with messages"),
  send_testing_data(<<>>, PurgeQueue),

  ct:comment("Purge queue"),
  TestMsgsCount = how_many_messages(),
  TestMsgsCount = esl_rabbitmq_client_worker:purge_queue(PurgeQueue),

  ct:comment("Clean up"),
  0 = esl_rabbitmq_client_worker:delete_queue(PurgeQueue),

  {comment, ""}.


-spec bind_and_unbind_queue(Config::config()) ->
  {comment, string()}.
bind_and_unbind_queue(Config) ->
  ct:comment("Create exchange"),
  Exchange = test_exchange(),
  esl_rabbitmq_client_worker:create_exchange( Exchange
                                            , <<"fanout">>
                                            , transient
                                            ),

  ct:comment("Create non-durable queue with randon name"),
  Queue = esl_rabbitmq_client_worker:create_queue(test_queue()),

  ct:comment("Bind ~p queue with ~p exchange", [Queue, Exchange]),
  RoutingKey = test_routing_key(),
  esl_rabbitmq_client_worker:bind_queue(Queue, Exchange, RoutingKey),
  true = binding_exist(Config, Queue, Exchange, RoutingKey),

  ct:comment("Unbind ~p queue", [Queue]),
  esl_rabbitmq_client_worker:unbind_queue(Queue, Exchange, RoutingKey),
  false = binding_exist(Config, Queue, Exchange, RoutingKey),

  ct:comment("Delete queue"),
  esl_rabbitmq_client_worker:delete_queue(Queue),

  ct:comment("Delete exchange"),
  esl_rabbitmq_client_worker:delete_exchange(Exchange),

  {comment, ""}.


-spec publish(Config::config()) ->
  {comment, string()}.
publish(_Config) ->
  Exchange = test_exchange(),
  Queue = test_queue(),
  RoutingKey = test_routing_key(),
  Payload = <<"Testing message">>,

  ct:comment("Create queue"),
  Queue = esl_rabbitmq_client_worker:create_queue(Queue),

  % Message should be routed to the given queue (`Queue')
  ct:comment("Publish to default exchange"),
  esl_rabbitmq_client_worker:publish(<<"">>, Queue, Payload),

  ct:comment("Create direct exchange"),
  esl_rabbitmq_client_worker:create_exchange(Exchange, <<"direct">>),

  ct:comment("Bind queue with direct exchange"),
  esl_rabbitmq_client_worker:bind_queue(Queue, Exchange, RoutingKey),

  ct:comment("Publish to direct exchange"),
  esl_rabbitmq_client_worker:publish(Exchange, RoutingKey, Payload),

  ct:comment("Clean up"),
  2 = esl_rabbitmq_client_worker:delete_queue(Queue),
  esl_rabbitmq_client_worker:delete_exchange(Exchange),

  {comment, ""}.


-spec consume(Config::config()) ->
  {comment, string()}.
consume(_Config) ->
  Exchange = test_exchange(),
  Queue = test_queue(),
  RoutingKey = test_routing_key(),
  TestMsgsCount = how_many_messages(),

  ct:comment("Create exchange and queue and bind them"),
  esl_rabbitmq_client_worker:create_exchange(Exchange, <<"direct">>),
  Queue = esl_rabbitmq_client_worker:create_queue(Queue),
  esl_rabbitmq_client_worker:bind_queue(Queue, Exchange, RoutingKey),

  ct:comment("Send test messages"),
  send_testing_data(Exchange, RoutingKey),

  ct:comment("Check there are messages when deleting the queue"),
  TestMsgsCount = esl_rabbitmq_client_worker:delete_queue(Queue),

  ct:comment("Recreate queue and bind it again to the exchange"),
  Queue = esl_rabbitmq_client_worker:create_queue(Queue),
  esl_rabbitmq_client_worker:bind_queue(Queue, Exchange, RoutingKey),

  ct:comment("Spawn messages handler function - will be used as a drainer"),
  MsgsHandler = spawn(fun basic_consume/0),

  ct:comment("Send test messages"),
  send_testing_data(Exchange, RoutingKey),

  ct:comment("Start consuming messages from queue"),
  esl_rabbitmq_client_worker:consume(Queue, MsgsHandler),

  % Since all the messages were consumed and acknowledged, there
  % should be 0 messages left in the queue when the queue is deleted.
  0 = esl_rabbitmq_client_worker:delete_queue(Queue),

  {comment, ""}.

%% ============================================================================
%% Private
%% ============================================================================
-spec test_exchange() ->
  binary().
test_exchange() ->
  <<"esl-rabbitmq-client-testing-exchange">>.


-spec test_queue() ->
  binary().
test_queue() ->
  <<"esl-rabbitmq-client-testing-queue">>.


-spec test_routing_key() ->
  binary().
test_routing_key() ->
  <<"esl-rabbitmq-client-testing-routing-key">>.


% `resource:exist/3' wrapper
-spec exchange_exist(Config::config(), Name::binary()) ->
  boolean().
exchange_exist(Config, Name) ->
  resource_exist(Config, exchange, Name).


% `resource:exist/3' wrapper
-spec queue_exist(Config::config(), Name::binary()) ->
  boolean().
queue_exist(Config, Name) ->
  resource_exist(Config, queue, Name).


-spec build_resource( Config::config()
                    , Kind::exchange | queue
                    , Name::binary()
                    ) ->
  resource().
build_resource(Config, Kind, Name) ->
  VHost = proplists:get_value(rmq_vhost, Config),
  #resource{virtual_host = VHost, kind = Kind, name = Name}.


-spec resource_exist( Config::config()
                    , Kind::exchange | queue
                    , Name::binary()
                    ) ->
  boolean().
resource_exist(Config, Kind, Name) ->
  Resource = build_resource(Config, Kind, Name),
  Module =
    case Kind of
      exchange -> rabbit_exchange;
      queue    -> rabbit_amqqueue
    end,
  LookupResult = rabbit_ct_broker_helpers:rpc( Config
                                             , 0
                                             , Module
                                             , lookup
                                             , [Resource]
                                             ),
  case LookupResult of
    {ok, _} -> true;
    {error, not_found} -> false
  end.


-spec binding_exist( Config::config()
                   , Queue::binary()
                   , Exchange::binary()
                   , RoutingKey::binary()
                   ) ->
  boolean().
binding_exist(Config, Queue, Exchange, RoutingKey) ->
  ExchResource = build_resource(Config, exchange, Exchange),
  QueueResource = build_resource(Config, queue, Queue),
  Binding = #binding{ source = ExchResource
                    , destination = QueueResource
                    , key = RoutingKey
                    },
  Exist = rabbit_ct_broker_helpers:rpc( Config
                                      , 0
                                      , rabbit_binding
                                      , exists
                                      , [Binding]
                                      ),
  case Exist of
    true -> true;
    _ -> false
  end.


-spec send_testing_data(Exchange::binary(), RoutingKey::binary()) ->
  ok.
send_testing_data(Exchange, RoutingKey) ->
  lists:foreach(fun(N) ->
                  NBin = integer_to_binary(N),
                  MsgBin = <<"Testing msg ", NBin/binary>>,
                  esl_rabbitmq_client_worker:publish( Exchange
                                                    , RoutingKey
                                                    , MsgBin
                                                    )
                end,
                lists:seq(1, how_many_messages())),
  ok.


-spec basic_consume() ->
  ok.
basic_consume() ->
  receive
    {'basic.consume_ok', _Message} ->
      ct:pal("Waiting for messages...", []),
      basic_consume();
    {'basic.cancel', _Message} ->
      ct:pal("Ok, bye", []);
    { {'basic.deliver', BasicDeliver}, {amqp_msg, AMQPMsg} } ->
      DTag = proplists:get_value(delivery_tag, BasicDeliver),
      Payload = proplists:get_value(payload, AMQPMsg),
      ct:pal("Payload -> ~p", [Payload]),
      esl_rabbitmq_client_worker:acknowledge_message(DTag),
      basic_consume()
  end.


-spec how_many_messages() ->
  integer().
how_many_messages() ->
  100.
