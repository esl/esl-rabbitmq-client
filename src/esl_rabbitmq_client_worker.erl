-module(esl_rabbitmq_client_worker).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([ create_exchange/1
        , create_exchange/2
        , create_exchange/3
        , delete_exchange/1
        ]).
-export([ create_queue/0
        , create_queue/1
        , create_queue/2
        , purge_queue/1
        , delete_queue/1
        , bind_queue/3
        , unbind_queue/3
        ]).
-export([ publish/3
        , consume/2
        , acknowledge_message/1
        ]).
%% `gen_server' behaviour callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        , terminate/2
        ]).


%% Types definition
-type state() :: #{ connection := pid()
                  , channel := pid()
                  }.

%% =============================================================================
%% API
%% =============================================================================
-spec start_link() ->
  {ok, pid()} | {error, {already_started, pid()} | term()} | ignore.
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec create_exchange(Name::binary()) ->
  ok.
create_exchange(Name) when is_binary(Name) ->
  create_exchange(Name, <<"direct">>, transient).

-spec create_exchange(Name::binary(), Type::binary()) ->
  ok.
create_exchange(Name, Type) when is_binary(Name),
                                 is_binary(Type) ->
  create_exchange(Name, Type, transient).


-spec create_exchange( Name::binary()
                     , Type::binary()
                     , Durable::durable | transient | boolean()
                     ) ->
  ok.
create_exchange(Name, Type, durable) when is_binary(Name),
                                          is_binary(Type) ->
  create_exchange(Name, Type, true);
create_exchange(Name, Type, transient) when is_binary(Name),
                                            is_binary(Type) ->
  create_exchange(Name, Type, false);
create_exchange(Name, Type, Durable) when is_binary(Name),
                                          is_binary(Type),
                                          is_boolean(Durable) ->
  {ok, ExchangeDeclare} = esl_rabbitmq_client_amqp:exchange_declare( Name
                                                                   , Type
                                                                   , Durable
                                                                   ),
  gen_server:call(?MODULE, {create_exchange, ExchangeDeclare}).


-spec delete_exchange(Name::binary()) ->
  ok.
delete_exchange(Name) when is_binary(Name) ->
  {ok, ExchangeDelete} = esl_rabbitmq_client_amqp:exchange_delete(Name),
  gen_server:call(?MODULE, {delete_exchange, ExchangeDelete}).


% Create non-durable queue with a random name
-spec create_queue() ->
  binary().
create_queue() ->
  create_queue(<<>>, false).

-spec create_queue(Queue::binary()) ->
  binary().
create_queue(Queue) when is_binary(Queue) ->
  create_queue(Queue, false).


-spec create_queue(Queue::binary(), Durable::durable | transient | boolean()) ->
  binary().
create_queue(Queue, durable) when is_binary(Queue) ->
  create_queue(Queue, true);
create_queue(Queue, transient) when is_binary(Queue) ->
  create_queue(Queue, false);
create_queue(Queue, Durable) when is_binary(Queue),
                                 is_boolean(Durable) ->
  {ok, QueueDeclare} = esl_rabbitmq_client_amqp:queue_declare(Queue, Durable),
  gen_server:call(?MODULE, {create_queue, QueueDeclare}).


-spec purge_queue(Queue::binary()) ->
  MessageCount::integer().
purge_queue(Queue) when is_binary(Queue) ->
  {ok, QueuePurge} = esl_rabbitmq_client_amqp:queue_purge(Queue),
  gen_server:call(?MODULE, {purge_queue, QueuePurge}).


-spec delete_queue(Queue::binary()) ->
  MessageCount::integer().
delete_queue(Queue) when is_binary(Queue) ->
  {ok, QueueDelete} = esl_rabbitmq_client_amqp:queue_delete(Queue),
  gen_server:call(?MODULE, {delete_queue, QueueDelete}).


-spec bind_queue(Queue::binary(), Exchange::binary(), RoutingKey::binary()) ->
  ok.
bind_queue(Queue, Exchange, RoutingKey) when is_binary(Queue),
                                             is_binary(Exchange),
                                             is_binary(RoutingKey) ->
  {ok, QueueBind} = esl_rabbitmq_client_amqp:queue_bind( Queue
                                                       , Exchange
                                                       , RoutingKey
                                                       ),
  gen_server:call(?MODULE, {bind_queue, QueueBind}).


-spec unbind_queue(Queue::binary(), Exchange::binary(), RoutingKey::binary()) ->
  ok.
unbind_queue(Queue, Exchange, RoutingKey) when is_binary(Queue),
                                             is_binary(Exchange),
                                             is_binary(RoutingKey) ->
  {ok, QueueUnbind} = esl_rabbitmq_client_amqp:queue_unbind( Queue
                                                           , Exchange
                                                           , RoutingKey
                                                           ),
  gen_server:call(?MODULE, {unbind_queue, QueueUnbind}).


-spec publish(Exchange::binary(), RoutingKey::binary(), Payload::binary()) ->
  ok.
publish(Exchange, RoutingKey, Payload) when is_binary(Exchange),
                                            is_binary(RoutingKey),
                                            is_binary(Payload) ->
  {ok, Publish, Msg} = esl_rabbitmq_client_amqp:basic_publish( Exchange
                                                             , RoutingKey
                                                             , Payload
                                                             ),
  gen_server:cast(?MODULE, {publish, Publish, Msg}).


-spec consume( Queue::binary(), MessagesHandlerPid::pid()) ->
  ok.
consume(Queue, MessagesHandlerPid) when is_binary(Queue) ->
  {ok, QueueSubscription} = esl_rabbitmq_client_amqp:basic_consume(Queue),
  gen_server:call(?MODULE, {consume, QueueSubscription, MessagesHandlerPid}).


-spec acknowledge_message(DeliveryTag::integer()) ->
  ok.
acknowledge_message(DeliveryTag) ->
  {ok, BasicAck} = esl_rabbitmq_client_amqp:basic_ack(DeliveryTag),
  gen_server:cast(?MODULE, {acknowledge_message, BasicAck}).

%% =============================================================================
%% `gen_server' behaviour callbacks
%% =============================================================================
-spec init(Args::term()) ->
  {ok, state()}.
init(_Args) ->
  {ok, AMQPParamsNetwork} = esl_rabbitmq_client_amqp:amqp_params_network(),
  {ok, Connection} = amqp_connection:start(AMQPParamsNetwork),
  {ok, Channel} = amqp_connection:open_channel(Connection),
  {ok, _MsgsHandlerSupPid} = esl_rabbitmq_client_msg_handler_sup:start_link(),

  {ok, #{connection => Connection, channel => Channel}}.


-spec handle_call( Request::{atom(), term()} | {atom(), term(), term()}
                 , From::tuple()
                 , State::state()
                 ) ->
  {noreply, state()} | {reply, binary() | ok, state()}.
handle_call( {create_exchange, ExchangeDeclare}
           , _From
           , State = #{channel := Channel}
           ) ->
  {ok, ExchangeDeclareOK} = esl_rabbitmq_client_amqp:exchange_declare_ok(),
  ExchangeDeclareOK = amqp_channel:call(Channel, ExchangeDeclare),
  {reply, ok, State};
handle_call( {delete_exchange, ExchangeDelete}
           , _From
           , State = #{channel := Channel}
           ) ->
  {ok, ExchangeDeleteOK} = esl_rabbitmq_client_amqp:exchange_delete_ok(),
  ExchangeDeleteOK = amqp_channel:call(Channel, ExchangeDelete),
  {reply, ok, State};
handle_call( {create_queue, QueueDeclare}
           , _From
           , State = #{channel := Channel}
           ) ->
  QueueDeclareOK = amqp_channel:call(Channel, QueueDeclare),
  {ok, Queue} =
    esl_rabbitmq_client_amqp:queue_declare_queue_name(QueueDeclareOK),
  {reply, Queue, State};
handle_call( {purge_queue, QueuePurge}
           , _From
           , State = #{channel := Channel}
           ) ->
  QueuePurgeOK = amqp_channel:call(Channel, QueuePurge),
  {ok, MessageCount} =
    esl_rabbitmq_client_amqp:queue_purge_message_count(QueuePurgeOK),
  {reply, MessageCount, State};
handle_call( {delete_queue, QueueDelete}
           , _From
           , State = #{channel := Channel}
           ) ->
  QueueDeleteOK = amqp_channel:call(Channel, QueueDelete),
  {ok, MessageCount} =
    esl_rabbitmq_client_amqp:queue_delete_message_count(QueueDeleteOK),
  {reply, MessageCount, State};
handle_call( {bind_queue, QueueBind}
           , _From
           , State = #{channel := Channel}
           ) ->
  {ok, QueueBindOK} = esl_rabbitmq_client_amqp:queue_bind_ok(),
  QueueBindOK = amqp_channel:call(Channel, QueueBind),
  {reply, ok, State};
handle_call( {unbind_queue, QueueUnbind}
           , _From
           , State = #{channel := Channel}
           ) ->
  {ok, QueueUnbindOK} = esl_rabbitmq_client_amqp:queue_unbind_ok(),
  QueueUnbindOK = amqp_channel:call(Channel, QueueUnbind),
  {reply, ok, State};
handle_call( {consume, QueueSubscription, MsgsHandlerPid}
           , _From
           , State = #{channel := Channel}
           ) ->
  {ok, BasicConsumeOK} =
    esl_rabbitmq_client_amqp:basic_consume_ok(QueueSubscription),
  {ok, MsgsHandlerWorkerPid} =
    esl_rabbitmq_client_msg_handler_sup:start_child(MsgsHandlerPid),
  BasicConsumeOK = amqp_channel:subscribe( Channel
                                         , QueueSubscription
                                         , MsgsHandlerWorkerPid
                                         ),
  {reply, ok, State};
handle_call(Request, From, State) ->
  error_logger:info_msg(
    "Unknown request -> ~p received from ~p when state was -> ~n~p",
    [Request, From, State]),
  {noreply, State}.


-spec handle_info(Info::timeout | term(), State::state()) ->
  {noreply, state()}.
handle_info(Info, State) ->
  error_logger:info_msg(
    "Unknown information received -> ~p when state was -> ~n~p",
    [Info, State]),
  {noreply, State}.

-spec handle_cast(Request::term(), State::state()) ->
  {noreply, state()}.
handle_cast({publish, Publish, Msg}, State = #{channel := Channel}) ->
  amqp_channel:cast(Channel, Publish, Msg),
  {noreply, State};
handle_cast({acknowledge_message, BasicAck}, State = #{channel := Channel}) ->
  amqp_channel:cast(Channel, BasicAck),
  {noreply, State};
handle_cast(Request, State) ->
  error_logger:info_msg(
    "Unknown async request -> ~p when state was ~p~n",
    [Request, State]),
  {noreply, State}.


-spec code_change( OldVsn::term() | {down, term()}
                 , State::state()
                 , Extra::term()
                 ) ->
  {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


-spec terminate( Reason::normal | shutdown | {shutdown, term()} | term()
               , State::state()
               ) ->
  ok.
terminate(Reason, State) ->
  error_logger:info_msg(
    "Terminating application with Reason -> ~p when state was ~p~n",
    [Reason, State]),
  ok.
