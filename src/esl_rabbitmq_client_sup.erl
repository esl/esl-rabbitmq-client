-module(esl_rabbitmq_client_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).
%% `supervisor' behaviour callbacks
-export([init/1]).

%% =============================================================================
%% API
%% =============================================================================
-spec start_link() ->
  {ok, pid()}.
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init(Args::term()) ->
  {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(_Args) ->
  SupervisorFlags = #{ strategy => one_for_one
                     , intensity => 5
                     , period => 30
                     },
  ChildSpec = #{ id => esl_rabbitmq_client_worker
               , start => {esl_rabbitmq_client_worker, start_link, []}
               , restart => permanent
               , shutdown => 5000
               , type => worker
               , modules => [esl_rabbitmq_client_worker]
               },
  {ok, {SupervisorFlags, [ChildSpec]}}.
