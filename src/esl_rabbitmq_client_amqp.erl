-module(esl_rabbitmq_client_amqp).

-include_lib("amqp_client/include/amqp_client.hrl").

%% AMQP params network
-export([ amqp_params_network/0
        ]).
%% Exchanges
-export([ exchange_declare/3
        , exchange_declare_ok/0
        , exchange_delete/1
        , exchange_delete_ok/0
        ]).
%% Queue
-export([ queue_declare/2
        , queue_declare_queue_name/1
        , queue_purge/1
        , queue_purge_message_count/1
        , queue_delete/1
        , queue_delete_message_count/1
        , queue_bind/3
        , queue_bind_ok/0
        , queue_unbind/3
        , queue_unbind_ok/0
        ]).
%% Basic
-export([ basic_publish/3
        , basic_consume/1
        , basic_consume_ok/1
        , basic_ack/1
        ]).
%% Format converters
-export([ from_basic_consume_ok/1
        , from_basic_deliver/2
        , from_basic_cancel/1
        ]).

%% Types definition
-type amqp_params_network() :: #amqp_params_network{}.
-type exchange_declare()    :: #'exchange.declare'{}.
-type exchange_declare_ok() :: #'exchange.declare_ok'{}.
-type exchange_delete()     :: #'exchange.delete'{}.
-type exchange_delete_ok()  :: #'exchange.delete_ok'{}.
-type queue_declare()       :: #'queue.declare'{}.
-type queue_declare_ok()    :: #'queue.declare_ok'{}.
-type queue_purge()         :: #'queue.purge'{}.
-type queue_purge_ok()      :: #'queue.purge_ok'{}.
-type queue_delete()        :: #'queue.delete'{}.
-type queue_delete_ok()     :: #'queue.delete_ok'{}.
-type queue_bind()          :: #'queue.bind'{}.
-type queue_bind_ok()       :: #'queue.bind_ok'{}.
-type queue_unbind()        :: #'queue.unbind'{}.
-type queue_unbind_ok()     :: #'queue.unbind_ok'{}.
-type basic_publish()       :: #'basic.publish'{}.
-type amqp_msg()            :: #amqp_msg{}.
-type basic_consume()       :: #'basic.consume'{}.
-type basic_consume_ok()    :: #'basic.consume_ok'{}.
-type basic_deliver()       :: #'basic.deliver'{}.
-type basic_cancel()        :: #'basic.cancel'{}.
-type basic_ack()           :: #'basic.ack'{}.

%% =============================================================================
%% AMQP params network
%% =============================================================================
-spec amqp_params_network() ->
  {ok, amqp_params_network()}.
amqp_params_network() ->
  ParamsNetwork =
    case esl_rabbitmq_client_config:get(uri_spec, undefined) of
      undefined ->
        #amqp_params_network
         { username = esl_rabbitmq_client_config:get(username, <<"guest">>)
         , password = esl_rabbitmq_client_config:get(password, <<"guest">>)
         , virtual_host = esl_rabbitmq_client_config:get(virtual_host, <<"/">>)
         , host = esl_rabbitmq_client_config:get(host, "localhost")
         , port = esl_rabbitmq_client_config:get(port, undefined)
         };
      URI ->
        {ok, NetworkParams} = amqp_uri:parse(URI),
        NetworkParams
    end,
  AuthMechanismsDefault = [ fun amqp_auth_mechanisms:plain/3
                          %, fun amqp_auth_mechanisms:amqpplain/3
                          ],
  % Read and set custom parameters if any, otherwise use default values
  AMQPParamsNetwork = ParamsNetwork#amqp_params_network
    { channel_max = esl_rabbitmq_client_config:get(channel_max, 0)
    , frame_max = esl_rabbitmq_client_config:get(frame_max, 0)
    , heartbeat = esl_rabbitmq_client_config:get(heartbeat, 10)
    , connection_timeout = esl_rabbitmq_client_config:get( connection_timeout
                                                         , infinity
                                                         )
    , ssl_options = esl_rabbitmq_client_config:get(ssl_options, none)
    , auth_mechanisms = esl_rabbitmq_client_config:get( auth_mechanisms
                                                      , AuthMechanismsDefault
                                                      )
    , client_properties = esl_rabbitmq_client_config:get(client_properties, [])
    , socket_options = esl_rabbitmq_client_config:get(socket_options, [])
    },
  {ok, AMQPParamsNetwork}.

%% =============================================================================
%% Exchanges
%% =============================================================================
-spec exchange_declare(Name::binary(), Type::binary(), Durable::boolean()) ->
  {ok, exchange_declare()}.
exchange_declare(Name, Type, Durable) ->
  ExchangeDeclare = #'exchange.declare'{ exchange = Name
                                       , type = Type
                                       , durable = Durable
                                       },
  {ok, ExchangeDeclare}.


-spec exchange_declare_ok() ->
  {ok, exchange_declare_ok()}.
exchange_declare_ok() ->
  {ok, #'exchange.declare_ok'{}}.


-spec exchange_delete(Name::binary()) ->
  {ok, exchange_delete()}.
exchange_delete(Name) ->
  {ok, #'exchange.delete'{exchange = Name}}.

-spec exchange_delete_ok() ->
  {ok, exchange_delete_ok()}.
exchange_delete_ok() ->
  {ok, #'exchange.delete_ok'{}}.

%% =============================================================================
%% Queues
%% =============================================================================
-spec queue_declare(Queue::binary(), Durable::boolean()) ->
  {ok, queue_declare()}.
queue_declare(Queue, Durable) ->
  {ok, #'queue.declare'{queue = Queue , durable = Durable}}.


-spec queue_declare_queue_name(QueueDeclareResult::queue_declare_ok()) ->
  {ok, binary()}.
queue_declare_queue_name(#'queue.declare_ok'{queue = Queue}) ->
  {ok, Queue}.


-spec queue_purge(Queue::binary()) ->
  {ok, queue_purge()}.
queue_purge(Queue) ->
  {ok, #'queue.purge'{queue = Queue}}.


-spec queue_purge_message_count(QueuePurgeOK::queue_purge_ok()) ->
  {ok, integer()}.
queue_purge_message_count(#'queue.purge_ok'{message_count = MessageCount}) ->
  {ok, MessageCount}.


-spec queue_delete(Queue::binary()) ->
  {ok, queue_delete()}.
queue_delete(Queue) ->
  {ok, #'queue.delete'{queue = Queue}}.


-spec queue_delete_message_count(QueueDeleteOK::queue_delete_ok()) ->
  {ok, integer()}.
queue_delete_message_count(#'queue.delete_ok'{message_count = MessageCount}) ->
  {ok, MessageCount}.


-spec queue_bind(Queue::binary(), Exchange::binary(), RoutingKey::binary()) ->
  {ok, queue_bind()}.
queue_bind(Queue, Exchange, RoutingKey) ->
  QueueBind = #'queue.bind'{ queue = Queue
                           , exchange = Exchange
                           , routing_key = RoutingKey
                           },
  {ok, QueueBind}.


-spec queue_bind_ok() ->
  {ok, queue_bind_ok()}.
queue_bind_ok() ->
  {ok, #'queue.bind_ok'{}}.


-spec queue_unbind(Queue::binary(), Exchange::binary(), RoutingKey::binary()) ->
  {ok, queue_unbind()}.
queue_unbind(Queue, Exchange, RoutingKey) ->
  QueueUnbind = #'queue.unbind'{ queue = Queue
                               , exchange = Exchange
                               , routing_key = RoutingKey
                               },
  {ok, QueueUnbind}.


-spec queue_unbind_ok() ->
  {ok, queue_unbind_ok()}.
queue_unbind_ok() ->
  {ok, #'queue.unbind_ok'{}}.

%% =============================================================================
%% Basic
%% =============================================================================
-spec basic_publish( Exchange::binary()
                   , RoutingKey::binary()
                   , Payload::binary()
                   ) ->
  {ok, basic_publish(), amqp_msg()}.
basic_publish(Exchange, RoutingKey, Payload) ->
  Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
  Msg = #amqp_msg{payload = Payload},
  {ok, Publish, Msg}.


-spec basic_consume(Queue::binary()) ->
  {ok, basic_consume()}.
basic_consume(Queue) ->
  {ok, #'basic.consume'{queue = Queue, consumer_tag = consumer_tag()}}.


-spec basic_consume_ok(QueueSubscription::basic_consume()) ->
  {ok, basic_consume_ok()}.
basic_consume_ok(#'basic.consume'{consumer_tag = CTag}) ->
  {ok, #'basic.consume_ok'{consumer_tag = CTag}}.


-spec basic_ack(DeliveryTag::integer()) ->
  {ok, basic_ack()}.
basic_ack(DeliveryTag) ->
  {ok, #'basic.ack'{delivery_tag = DeliveryTag}}.

%% =============================================================================
%% Format converters
%% =============================================================================
-spec from_basic_consume_ok(BasicConsumeOK::basic_consume_ok()) ->
  {atom(), proplists:proplist()}.
from_basic_consume_ok(BasicConsumeOK) ->
  { 'basic.consume_ok',
    amqp_primitive_to_list( record_info(fields, 'basic.consume_ok')
                          , BasicConsumeOK
                          )
  }.


-spec from_basic_deliver(BasicDeliver::basic_deliver(), AMQPMsg::amqp_msg()) ->
  {{atom(), proplists:proplist()}, {atom(), proplists:proplist()}}.
from_basic_deliver(BasicDeliver, AMQPMsg) ->
  PropsList = amqp_primitive_to_list( record_info(fields, 'P_basic')
                                    , AMQPMsg#amqp_msg.props
                                    ),
  NewAMQPMsg = AMQPMsg#amqp_msg{props = {'P_basic', PropsList}},
  BasicDeliverList =
    amqp_primitive_to_list(record_info(fields, 'basic.deliver'), BasicDeliver),
  AMQPMsgList =
    amqp_primitive_to_list(record_info(fields, amqp_msg), NewAMQPMsg),
  {{'basic.deliver', BasicDeliverList}, {amqp_msg, AMQPMsgList}}.


-spec from_basic_cancel(BasicCancel::basic_cancel()) ->
  {atom(), proplists:proplist()}.
from_basic_cancel(BasicCancel) ->
  { 'basic.cancel',
    amqp_primitive_to_list(record_info(fields, 'basic.cancel'), BasicCancel)
  }.

%% =============================================================================
%% Private
%% =============================================================================
-spec consumer_tag() ->
  binary().
consumer_tag() ->
  UniqueIntBin = integer_to_binary(erlang:unique_integer()),
  <<"esl-rabbitmq-client-consumer-tag", UniqueIntBin/binary>>.


-spec amqp_primitive_to_list( AMQPPrimitiveFields::[atom()]
                            , AMQPPrimitiveData::tuple()
                            ) ->
  [{atom(), _}].
amqp_primitive_to_list(AMQPPrimitiveFields, AMQPPrimitiveData) ->
  lists:zip( AMQPPrimitiveFields
           , tl(tuple_to_list(AMQPPrimitiveData))
           ).
