-module(esl_rabbitmq_client).
-behaviour(application).

%% API
-export([start/0]).
%% `application' behaviour callbacks
-export([start/2, stop/1]).

%% =============================================================================
%% API
%% =============================================================================
-spec start() ->
  {ok, [atom()]} | {error, term()}.
start() ->
  application:ensure_all_started(esl_rabbitmq_client).

%% =============================================================================
%% `application' behaviour callbacks
%% =============================================================================
-spec start(StartType::application:start_type(), StartArgs::term()) ->
  {ok, pid()}.
start(_StartType, _StartArgs) ->
  esl_rabbitmq_client_sup:start_link().

-spec stop(State::term()) ->
  ok.
stop(_State) ->
  ok.
