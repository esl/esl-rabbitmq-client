-module(esl_rabbitmq_client_meta_SUITE).

-include_lib("mixer/include/mixer.hrl").
-mixin([ktn_meta_SUITE]).

-export([init_per_suite/1]).

init_per_suite(Config) -> [{base_dir, "../.."} | Config].
