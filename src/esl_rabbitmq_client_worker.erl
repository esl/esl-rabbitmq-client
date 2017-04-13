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
        , bind_queue/2
        , bind_queue/3
        , unbind_queue/2
        , unbind_queue/3
        ]).
-export([ publish/1
        , publish/3
        , publish/4
        , consume/2
        , acknowledge_message/1
        , set_prefetch_count/1
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

%% First spec is for the cases the user wants to use parameters not supported
%% by create_exchange/[1, 2, 3] implementation (API). i.e:
%%
%%    create_exchange([ {exchange, <<"myexch">>}
%%                    , {auto_delete, true}
%%                    , {nowait, true}
%%                    ]).
%% ---
%% The second one is for the cases the user just wants to set the name and use
%% the default values for the rest of the parameters. i.e:
%%
%%    create_exchange(<<"myexch">>).
%% ---
-spec create_exchange(ExchangeParams::proplists:proplist()) -> ok;
                     (Name::binary()) -> ok.
create_exchange(ExchangeParams) when is_list(ExchangeParams) ->
  {ok, ExchangeDeclare} =
    esl_rabbitmq_client_amqp:exchange_declare(ExchangeParams),
  gen_server:call(?MODULE, {create_exchange, ExchangeDeclare});
create_exchange(Exchange) when is_binary(Exchange) ->
  create_exchange(Exchange, <<"direct">>, transient).

-spec create_exchange(Exchange::binary(), Type::binary()) ->
  ok.
create_exchange(Exchange, Type) when is_binary(Exchange),
                                     is_binary(Type) ->
  create_exchange(Exchange, Type, transient).


-spec create_exchange( Exchange::binary()
                     , Type::binary()
                     , Durable::durable | transient | boolean()
                     ) ->
  ok.
create_exchange(Exchange, Type, durable) when is_binary(Exchange),
                                              is_binary(Type) ->
  create_exchange(Exchange, Type, true);
create_exchange(Exchange, Type, transient) when is_binary(Exchange),
                                                is_binary(Type) ->
  create_exchange(Exchange, Type, false);
create_exchange(Exchange, Type, Durable) when is_binary(Exchange),
                                              is_binary(Type),
                                              is_boolean(Durable) ->
  create_exchange([ {exchange, Exchange}
                  , {type, Type}
                  , {durable, Durable}
                  ]).


-spec delete_exchange(Name::binary()) ->
  ok.
delete_exchange(Name) when is_binary(Name) ->
  {ok, ExchangeDelete} = esl_rabbitmq_client_amqp:exchange_delete(Name),
  gen_server:call(?MODULE, {delete_exchange, ExchangeDelete}).


%% Create non-durable queue with a random name
-spec create_queue() ->
  binary().
create_queue() ->
  create_queue(<<>>, false).


%% First spec is for the cases the user wants to use parameters not supported
%% by create_queue/[0, 1, 2] implementation (API). i.e:
%%
%%    create_queue([ {queue, <<"myexch">>}
%%                 , {auto_delete, true}
%%                 , {durable, true}
%%                 ]).
%% ---
%% The second one is for the cases the user just wants to set the name and use
%% the default values for the rest of the parameters. i.e:
%%
%%    create_queue(<<"my-queue">>).
%% ---
-spec create_queue(Queue::binary()) -> binary();
                  (QueueParams::proplists:proplist()) -> binary().
create_queue(Queue) when is_binary(Queue) ->
  create_queue(Queue, false);
create_queue(QueueParams) when is_list(QueueParams) ->
  {ok, QueueDeclare} = esl_rabbitmq_client_amqp:queue_declare(QueueParams),
  gen_server:call(?MODULE, {create_queue, QueueDeclare}).


-spec create_queue(Queue::binary(), Durable::durable | transient | boolean()) ->
  binary().
create_queue(Queue, durable) when is_binary(Queue) ->
  create_queue(Queue, true);
create_queue(Queue, transient) when is_binary(Queue) ->
  create_queue(Queue, false);
create_queue(Queue, Durable) when is_binary(Queue),
                                 is_boolean(Durable) ->
  create_queue([{queue, Queue}, {durable, Durable}]).


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


% Useful for `fanout' exchanges where the routing_key is ignored
-spec bind_queue(Queue::binary(), Exchange::binary()) ->
  ok.
bind_queue(Queue, Exchange) when is_binary(Queue),
                                 is_binary(Exchange) ->
  bind_queue(Queue, Exchange, <<>>).


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


% bind_queue/2 counterpart
-spec unbind_queue(Queue::binary(), Exchange::binary()) ->
  ok.
unbind_queue(Queue, Exchange) ->
  unbind_queue(Queue, Exchange, <<>>).


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


-spec publish({ PublishParams::proplists:proplist()
              , MsgPropsParams::proplists:proplist()
              , Payload::term()
              }) ->
  ok.
publish({PublishParams, MsgPropsParams, Payload}) ->
  {ok, Publish, Msg} = esl_rabbitmq_client_amqp:basic_publish( PublishParams
                                                             , MsgPropsParams
                                                             , Payload
                                                             ),
  gen_server:cast(?MODULE, {publish, Publish, Msg}).


-spec publish(Exchange::binary(), RoutingKey::binary(), Payload::term()) ->
  ok.
publish(Exchange, RoutingKey, Payload) when is_binary(Exchange),
                                            is_binary(RoutingKey) ->
  publish(Exchange, RoutingKey, Payload, false).


-spec publish( Exchange::binary()
             , RoutingKey::binary()
             , Payload::term()
             , Persistent::boolean() | 1 | 2
             ) ->
  ok.
publish(Exchange, RoutingKey, Payload, false) when is_binary(Exchange),
                                                       is_binary(RoutingKey) ->
  publish(Exchange, RoutingKey, Payload, 1);
publish(Exchange, RoutingKey, Payload, true) when is_binary(Exchange),
                                                     is_binary(RoutingKey) ->
  publish(Exchange, RoutingKey, Payload, 2);
publish(Exchange, RoutingKey, Payload, DMode) when is_binary(Exchange),
                                                   is_binary(RoutingKey),
                                                   is_number(DMode) ->
  publish({ [ {exchange, Exchange} , {routing_key, RoutingKey} ] % basic.publish
          , [ {delivery_mode, DMode} ] % P_basic
          , Payload
          }).


-spec consume(Queue::binary(), MessagesHandlerPid::pid()) ->
  ok.
consume(Queue, MessagesHandlerPid) when is_binary(Queue),
                                        is_pid(MessagesHandlerPid) ->
  {ok, QueueSubscription} = esl_rabbitmq_client_amqp:basic_consume(Queue),
  gen_server:call(?MODULE, {consume, QueueSubscription, MessagesHandlerPid}).


-spec acknowledge_message(DeliveryTag::integer()) ->
  ok.
acknowledge_message(DeliveryTag) when is_number(DeliveryTag) ->
  {ok, BasicAck} = esl_rabbitmq_client_amqp:basic_ack(DeliveryTag),
  gen_server:cast(?MODULE, {acknowledge_message, BasicAck}).


-spec set_prefetch_count(Count::integer()) ->
  ok.
set_prefetch_count(Count) when is_number(Count) ->
  {ok, BasicQos} = esl_rabbitmq_client_amqp:basic_qos(Count),
  gen_server:call(?MODULE, {set_prefetch_count, BasicQos}).

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
handle_call( {set_prefetch_count, BasicQos}
           , _From
           , State = #{channel := Channel}
           ) ->
  {ok, BasicQosOK} = esl_rabbitmq_client_amqp:basic_qos_ok(),
  BasicQosOK = amqp_channel:call(Channel, BasicQos),
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
terminate(Reason, State = #{channel := _Channel, connection := Connection}) ->
  % Clean up
  amqp_connection:close(Connection),
  error_logger:info_msg(
    "Terminating application with Reason -> ~p when state was ~p~n",
    [Reason, State]),
  ok.
