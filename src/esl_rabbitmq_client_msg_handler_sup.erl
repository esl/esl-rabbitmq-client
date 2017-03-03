-module(esl_rabbitmq_client_msg_handler_sup).
-behaviour(supervisor).

%% API
-export([start_link/0, start_child/1]).
%% `supervisor' behaviour callbacks
-export([init/1]).

%% =============================================================================
%% API
%% =============================================================================
-spec start_link() ->
  {ok, pid()}.
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).


-spec start_child(MsgsHandlerPid::pid()) ->
  {ok, pid()}.
start_child(MsgsHandlerPid) ->
  supervisor:start_child(?MODULE, [MsgsHandlerPid]).

%% =============================================================================
%% `supervisor' behaviour callbacks
%% =============================================================================
-spec init(Args::term()) ->
  {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(_Args) ->
  SupervisorFlags = #{ strategy => simple_one_for_one
                     , intensity => 5
                     , period => 30
                     },
  Module = esl_rabbitmq_client_msg_handler,
  ChildSpec = #{ id => Module
               , start => { Module
                          , start_link
                          , []
                          }
               , restart => permanent
               , shutdown => 5000
               , type => worker
               , modules => [Module]
               },
  {ok, {SupervisorFlags, [ChildSpec]}}.
