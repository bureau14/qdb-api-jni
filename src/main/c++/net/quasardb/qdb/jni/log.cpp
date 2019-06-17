#include "log.h"
#include "env.h"
#include "string.h"
#include "object.h"
#include <optional>
#include <string_view>
#include <iostream>
#include <vector>
#include <shared_mutex>
#include <algorithm>

static std::vector<qdb::jni::log::message_t> buffer;
static std::shared_mutex buffer_lock;
thread_local qdb_log_callback_id local_callback_id = 0;

qdb::jni::log::wrapper::wrapper() {
  if (local_callback_id == 0) {
    qdb_error_t error = qdb_log_add_callback(_callback, &local_callback_id);

    if (error) {
      fprintf(stderr, "a fatal error occured while registering QuasarDB logging engine: %s (%#x)\n", qdb_error(error), error);
      fflush(stderr);
      abort();
    }
  }
}

/* static */ void qdb::jni::log::wrapper::_callback(qdb_log_level_t log_level,
                                                    const unsigned long * /* date */,
                                                    unsigned long pid,
                                                    unsigned long tid,
                                                    const char * message_buffer,
                                                    size_t /* message_size */)
{
    message_t x { log_level,
                  static_cast<long>(pid),
                  static_cast<long>(tid),
                  std::string(message_buffer) };
    std::unique_lock guard(buffer_lock);

    buffer.push_back(x);
}

/* static */ void
qdb::jni::log::wrapper::flush(qdb::jni::env & env) {

  // Since this function is invoked a lot, and typically the buffer will be empty,
  // we first get a read lock, and then if (and only if) there is actually more than
  // one log available, we flush the log.
  //
  // There is still a chance of a race condition where multiple threads attempt to
  // acquire the exclusive lock when the buffer is non-empty, but we're optimizing
  // for the 'default' case of an empty buffer here.
  std::shared_lock shared_guard(buffer_lock);

  if (!buffer.empty()) {
    // Non-atomic upgrade to unique lock, for some reason it seems like the STL
    // does not have a mechanism to upgrade?
    shared_guard.unlock();
    std::unique_lock unique_guard(buffer_lock);
    wrapper::_do_flush(env);
  }
}

/* static */ void
qdb::jni::log::wrapper::_do_flush(qdb::jni::env & env) {
  std::for_each(buffer.begin(), buffer.end(), [& env] (message_t const & m) {
                                                _do_flush_message(env, m);
                                              });

  buffer.clear();

}

/* static */ void
qdb::jni::log::wrapper::_do_flush_message(qdb::jni::env & env, qdb::jni::log::message_t const & m) {
  static std::optional<jclass> qdbLogger = {};
  static std::optional<jfieldID> loggerField = {};
  static std::optional<jmethodID> logID = {};

  if (!qdbLogger) {
    qdbLogger.emplace(qdb::jni::introspect::lookup_class(env, "net/quasardb/qdb/Logger"));
    loggerField.emplace(qdb::jni::introspect::lookup_static_field(env, *qdbLogger, "logger", "Lorg/apache/logging/log4j/Logger;"));
    logID = introspect::lookup_static_method(env, *qdbLogger, "log", "(IJJLjava/lang/String;)V");
  }

  // Call error
  env.instance().CallStaticVoidMethod(*qdbLogger,
                                      *logID,
                                      m.level,
                                      m.pid,
                                      m.tid,
                                      env.instance().NewStringUTF(m.message.c_str()));

}
