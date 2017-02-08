-module(esl_rabbitmq_client_worker).
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

%% API
-export([ start_link/0
        , create_exchange/3
        , create_queue/0
        , create_queue/1
        , create_queue/2
        , bind_queue/3
        , consume_messages/2
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
  ExchangeDeclare = #'exchange.declare'{ exchange = Name
                                       , type = Type
                                       , durable = Durable
                                       },
  gen_server:call(?MODULE, {create_exchange, ExchangeDeclare}).


% Create non-durable queue with a random name
-spec create_queue() ->
  binary().
create_queue() ->
  create_queue(<<>>, false).

-spec create_queue(Name::binary()) ->
  binary().
create_queue(Name) when is_binary(Name) ->
  create_queue(Name, false).


-spec create_queue(Name::binary(), Durable::durable | transient | boolean()) ->
  binary().
create_queue(Name, durable) when is_binary(Name) ->
  create_queue(Name, true);
create_queue(Name, transient) when is_binary(Name) ->
  create_queue(Name, false);
create_queue(Name, Durable) when is_binary(Name),
                                 is_boolean(Durable) ->
  QueueDeclare = #'queue.declare'{queue = Name , durable = Durable},
  gen_server:call(?MODULE, {create_queue, QueueDeclare}).


-spec bind_queue(Exchange::binary(), Queue::binary(), RoutingKey::binary()) ->
  ok.
bind_queue(Exchange, Queue, RoutingKey) when is_binary(Exchange),
                                             is_binary(Queue),
                                             is_binary(RoutingKey) ->
  QueueBind = #'queue.bind'{ exchange = Exchange
                           , queue = Queue
                           , routing_key = RoutingKey
                           },
  gen_server:call(?MODULE, {bind_queue, QueueBind}).


-spec consume_messages( Queue::binary(), MessageHandler::pid()) ->
  ok.
consume_messages(Queue, MsgsHandler) when is_binary(Queue) ->
  QueueSubscription = #'basic.consume'{queue = Queue},
  gen_server:call(?MODULE, {consume_messages, QueueSubscription, MsgsHandler}).

%% =============================================================================
%% `gen_server' behaviour callbacks
%% =============================================================================
-spec init(Args::term()) ->
  {ok, state()}.
init(_Args) ->
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
  {ok, Connection} = amqp_connection:start(AMQPParamsNetwork),
  {ok, Channel} = amqp_connection:open_channel(Connection),

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
  #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare),
  {reply, ok, State};
handle_call( {create_queue, QueueDeclare}
           , _From
           , State = #{channel := Channel}
           ) ->
  #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, QueueDeclare),
  {reply, Queue, State};
handle_call( {bind_queue, QueueBind}
           , _From
           , State = #{channel := Channel}
           ) ->
  #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
  {reply, ok, State};
handle_call( {consume_messages, QueueSubscription, MsgsHandler}
           , _From
           , State = #{channel := Channel}
           ) ->
  #'basic.consume_ok'{} = amqp_channel:subscribe( Channel
                                                , QueueSubscription
                                                , MsgsHandler
                                                ),
  {reply, ok, State};
handle_call(Request, From, State) ->
  io:format( "Unknown request -> ~p received from ~p when state was -> ~n~p"
           , [Request, From, State]
           ),
  {noreply, State}.


-spec handle_info(Info::timeout | term(), State::state()) ->
  {noreply, state()}.
handle_info(Info, State) ->
  _ = io:format( "Unknown information received -> ~p when state was -> ~n~p"
               , [Info, State]
               ),
  {noreply, State}.

-spec handle_cast(Request::term(), State::state()) ->
  {noreply, state()}.
handle_cast(Request, State) ->
  io:format("Unknown async request -> ~p when state was ~p~n", [Request, State]),
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
  io:format( "Terminating application with Reason -> ~p when state was ~p~n"
           , [Reason, State]
           ),
  ok.
