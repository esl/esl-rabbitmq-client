-module(esl_rabbitmq_client_meta_SUITE).

-include_lib("mixer/include/mixer.hrl").
-mixin([{ktn_meta_SUITE
        , [ all/0
          , xref/1
          , dialyzer/1
          , elvis/1
          ]
}]).

-type config() :: [{atom(), term()}].

-export([init_per_suite/1, end_per_suite/1]).

-spec init_per_suite(Config::config()) ->
  config().
init_per_suite(Config) ->
  [{base_dir, "../.."} | Config].


-spec end_per_suite(Config::config()) ->
  config().
end_per_suite(Config) ->
  Config.
