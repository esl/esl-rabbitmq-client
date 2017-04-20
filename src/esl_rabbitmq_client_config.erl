-module(esl_rabbitmq_client_config).

-export([get/2]).

-spec get(Key::atom(), DefaultValue::term()) ->
  term().
get(Key, DefaultValue) ->
  application:get_env(esl_rabbitmq_client, Key, DefaultValue).
