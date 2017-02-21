PROJECT = esl_rabbitmq_client

DEPS = rabbit_common amqp_client
TEST_DEPS = mixer katana_test rabbitmq_ct_helpers rabbit
BUILD_DEPS = inaka_mk hexer_mk elvis_mk
SHELL_DEPS = sync

# DEPS
dep_rabbit_common = git https://github.com/rabbitmq/rabbitmq-common           rabbitmq_v3_6_6
dep_amqp_client = git https://github.com/rabbitmq/rabbitmq-erlang-client      rabbitmq_v3_6_6
# TEST_DEPS
dep_mixer = git https://github.com/inaka/mixer.git                            0.1.5
dep_katana_test = git https://github.com/inaka/katana-test.git                0.0.6
dep_rabbitmq_ct_helpers = git https://github.com/rabbitmq/rabbitmq-ct-helpers rabbitmq_v3_6_6
dep_rabbit = git https://github.com/rabbitmq/rabbitmq-server.git              rabbitmq_v3_6_6
# BUILD_DEPS
dep_inaka_mk    = git https://github.com/inaka/inaka.mk.git                   1.0.0
dep_hexer_mk = git https://github.com/inaka/hexer.mk.git                      1.1.0
dep_elvis_mk    = git https://github.com/inaka/elvis.mk.git                   1.0.0
# SHELL_DEPS
dep_sync = git https://github.com/rustyio/sync.git                            20846b0

DEP_PLUGINS = inaka_mk hexer_mk elvis_mk rabbit_common/mk/rabbitmq-plugin.mk

include erlang.mk

ERLC_OPTS := +warn_unused_vars +warn_export_all +warn_shadow_vars +warn_unused_import +warn_unused_function
ERLC_OPTS += +warn_bif_clash +warn_unused_record +warn_deprecated_function +warn_obsolete_guard +strict_validation
ERLC_OPTS += +warn_export_vars +warn_exported_vars +warn_missing_spec +warn_untyped_record +debug_info

SHELL_OPTS = -name esl_rabbitmq_client -boot start_sasl -s sync -s esl_rabbitmq_client
