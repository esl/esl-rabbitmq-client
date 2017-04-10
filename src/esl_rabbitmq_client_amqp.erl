-module(esl_rabbitmq_client_amqp).

-include_lib("amqp_client/include/amqp_client.hrl").

%% AMQP params network
-export([ amqp_params_network/0
        ]).
%% Exchanges
-export([ exchange_declare/1
        , exchange_declare_ok/0
        , exchange_delete/1
        , exchange_delete_ok/0
        ]).
%% Queue
-export([ queue_declare/1
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
        , basic_qos/1
        , basic_qos_ok/0
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
-type basic_qos()           :: #'basic.qos'{}.
-type basic_qos_ok()        :: #'basic.qos_ok'{}.

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
-spec exchange_declare(ExchangeParams::proplists:proplist()) ->
  {ok, exchange_declare()}.
exchange_declare(ExchangeParams) ->
  RecordFields = record_info(fields, 'exchange.declare'),
  % Exchange declaration with default values
  ExchangeDeclareList = primitive_to_list(RecordFields, #'exchange.declare'{}),
  % Set user defined parameters
  ExchangeDeclare = primitive_declare(ExchangeDeclareList, ExchangeParams),
  % Convert proplist back to primitive
  Values = [proplists:get_value(Key, ExchangeDeclare) || Key <- RecordFields],
  Exchange = list_to_tuple(['exchange.declare' | Values]),
  {ok, Exchange}.


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
-spec queue_declare(QueueParams::proplists:proplist()) ->
  {ok, queue_declare()}.
queue_declare(QueueParams) ->
  PrimitiveFields = record_info(fields, 'queue.declare'),
  % Queue declaration with default values
  QueueDeclareList = primitive_to_list(PrimitiveFields, #'queue.declare'{}),
  % Set user defined parameters
  QueueDeclare = primitive_declare(QueueDeclareList, QueueParams),
  % Convert proplist back to primitive
  Values = [proplists:get_value(K, QueueDeclare) || K <- PrimitiveFields],
  Queue = list_to_tuple(['queue.declare' | Values]),
  {ok, Queue}.


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
-spec basic_publish( PublishParams::proplists:prolist()
                   , Payload::term()
                   , MsgPropsParams::proplists:proplist()
                   ) ->
  {ok, basic_publish(), amqp_msg()}.
basic_publish(PublishParams, Payload, MsgPropsParams) ->
  % === basic.publish ===
  PublishFields = record_info(fields, 'basic.publish'),
  % basic.publish default values
  PublishDeclareList = primitive_to_list(PublishFields, #'basic.publish'{}),
  % Set user defined parameters
  PublishDeclare = primitive_declare(PublishDeclareList, PublishParams),
  % Convert proplist back to primitive
  PublishValues =
    [proplists:get_value(K, PublishDeclare) || K <- PublishFields],
  Publish = list_to_tuple(['basic.publish' | PublishValues]),

  % === P_basic ===
  PBasicFields = record_info(fields, 'P_basic'),
  % P_basic default values
  PBasicDeclareList = primitive_to_list(PBasicFields, #'P_basic'{}),
  % Set user defined parameters
  PBasicDeclare = primitive_declare(PBasicDeclareList, MsgPropsParams),
  % Convert proplist back to primitive
  PBasicValues = [proplists:get_value(K, PBasicDeclare) || K <- PBasicFields],
  PBasic = list_to_tuple(['P_basic' | PBasicValues]),

  % === amqp_msg ===
  Msg = #amqp_msg{payload = Payload, props = PBasic},

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


-spec basic_qos(Count::integer()) ->
  {ok, basic_qos()}.
basic_qos(Count) ->
  {ok, #'basic.qos'{prefetch_count = Count}}.


-spec basic_qos_ok() ->
  {ok, basic_qos_ok()}.
basic_qos_ok() ->
  {ok, #'basic.qos_ok'{}}.

%% =============================================================================
%% Format converters
%% =============================================================================
-spec from_basic_consume_ok(BasicConsumeOK::basic_consume_ok()) ->
  {atom(), proplists:proplist()}.
from_basic_consume_ok(BasicConsumeOK) ->
  { 'basic.consume_ok'
  , primitive_to_list(record_info(fields, 'basic.consume_ok'), BasicConsumeOK)
  }.


-spec from_basic_deliver(BasicDeliver::basic_deliver(), AMQPMsg::amqp_msg()) ->
  {{atom(), proplists:proplist()}, {atom(), proplists:proplist()}}.
from_basic_deliver(BasicDeliver, AMQPMsg) ->
  PropsList = primitive_to_list( record_info(fields, 'P_basic')
                               , AMQPMsg#amqp_msg.props
                               ),
  NewAMQPMsg = AMQPMsg#amqp_msg{props = {'P_basic', PropsList}},
  BasicDeliverList = primitive_to_list( record_info(fields, 'basic.deliver')
                                      , BasicDeliver
                                      ),
  AMQPMsgList = primitive_to_list(record_info(fields, amqp_msg), NewAMQPMsg),
  {{'basic.deliver', BasicDeliverList}, {amqp_msg, AMQPMsgList}}.


-spec from_basic_cancel(BasicCancel::basic_cancel()) ->
  {atom(), proplists:proplist()}.
from_basic_cancel(BasicCancel) ->
  { 'basic.cancel',
    primitive_to_list(record_info(fields, 'basic.cancel'), BasicCancel)
  }.

%% =============================================================================
%% Private
%% =============================================================================
-spec consumer_tag() ->
  binary().
consumer_tag() ->
  UniqueIntBin = integer_to_binary(erlang:unique_integer()),
  <<"esl-rabbitmq-client-consumer-tag", UniqueIntBin/binary>>.


-spec primitive_to_list(PrimitiveFields::[atom()], PrimitiveData::tuple()) ->
  [{atom(), _}].
primitive_to_list(PrimitiveFields, PrimitiveData) ->
  lists:zip(PrimitiveFields, tl(tuple_to_list(PrimitiveData))).

%% Iterate through each user defined parameter and use it for the declare
%% operation instead of the default values.
-spec primitive_declare( PrimitiveDeclareList::proplists:proplist()
                       , UserParams::proplists:proplist()
                       ) ->
  proplists:proplist().
primitive_declare(PrimitiveDeclareList, UserParams) ->
  lists:foldl(
    fun(Param = {Key, _Value}, TmpPrimitiveDeclare) ->
      lists:keyreplace(Key, 1, TmpPrimitiveDeclare, Param)
    end
  , PrimitiveDeclareList
  , UserParams
  ).
