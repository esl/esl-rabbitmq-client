-module(esl_rabbitmq_client_msg_handler).
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

%% API
-export([start_link/1]).
%% `gen_server' behaviour callbacks
-export([ init/1
        , handle_call/3
        , handle_info/2
        , handle_cast/2
        , code_change/3
        , terminate/2
        ]).

-type state() :: #{client_msgs_handler := pid()}.

%% =============================================================================
%% API
%% =============================================================================
-spec start_link(MsgsHandlerPid::pid()) ->
  {ok, pid()} | {error, {already_started, pid()} | term()} | ignore.
start_link(MsgsHandlerPid) ->
  gen_server:start_link(?MODULE, [MsgsHandlerPid], []).

%% =============================================================================
%% `gen_server' behaviour callbacks
%% =============================================================================
-spec init(Args::term()) ->
  {ok, state()}.
init([MsgsHandlerPid]) ->
  {ok, #{client_msgs_handler => MsgsHandlerPid}}.


-spec handle_call(Request::term(), From::tuple(), State::state()) ->
  {noreply, state()}.
handle_call(Request, From, State) ->
  error_logger:info_msg(
    "Unknown request -> ~p from -> ~p when state was -> ~p~n"
  , [Request, From, State]
  ),
  {noreply, State}.


-spec handle_info(Info::timeout | term(), State::state()) ->
  {noreply, state()}.
handle_info( BasicConsumeOK = #'basic.consume_ok'{}
           , State = #{client_msgs_handler := MsgsHandlerPid}
           ) ->
  NewMsg = esl_rabbitmq_client_amqp:from_basic_consume_ok(BasicConsumeOK),
  MsgsHandlerPid ! NewMsg,
  {noreply, State};
handle_info( {BasicDeliver = #'basic.deliver'{}, AMQPMsg = #amqp_msg{}}
           , State = #{client_msgs_handler := MsgsHandlerPid}
           ) ->
  NewMsg = esl_rabbitmq_client_amqp:from_basic_deliver(BasicDeliver, AMQPMsg),
  MsgsHandlerPid ! NewMsg,
  {noreply, State};
handle_info( BasicCancel = #'basic.cancel'{}
           , State = #{client_msgs_handler := MsgsHandlerPid}
           ) ->
  NewMsg = esl_rabbitmq_client_amqp:from_basic_cancel(BasicCancel),
  MsgsHandlerPid ! NewMsg,
  {noreply, State};
handle_info(Info, State) ->
  error_logger:info_msg( "Unknown Info -> ~p when state was -> ~p~n"
                       , [Info, State]
                       ),
  {noreply, State}.


-spec handle_cast(Request::term(), State::state()) ->
  {noreply, state()}.
handle_cast(Request, State) ->
  error_logger:info_msg( "Unknown Request -> ~p when state was -> ~p~n"
                       , [Request, State]
                       ),
  {noreply, State}.


-spec code_change(OldVsn::term(), State::state(), Extra::term()) ->
  {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


-spec terminate(Reason::term(), State::state()) ->
  ok.
terminate(Reason, State) ->
  error_logger:info_msg( "Terminating with reason -> ~p when state was -> ~p~n"
                       , [Reason, State]
                       ),
  ok.
