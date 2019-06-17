#include "log.h"
#include "env.h"
#include "string.h"
#include "object.h"
#include <optional>
#include <iostream>
#include <vector>
#include <shared_mutex>
#include <algorithm>
#include <time.h>
#include <string.h>

static std::vector<qdb::jni::log::message_t> buffer;
static std::shared_mutex buffer_lock;
static qdb_log_callback_id local_callback_id = 0;

/* static */ void
qdb::jni::log::ensure_callback(qdb::jni::env & env) {
  if (local_callback_id == 0) {
    qdb_error_t error = qdb_log_add_callback(_callback, &local_callback_id);

    printf("callback id = %d\n", local_callback_id);
    fflush(stdout);

    if (error) {
      fprintf(stderr, "a fatal error occured while registering QuasarDB logging engine: %s (%#x)\n", qdb_error(error), error);
      fflush(stderr);
      abort();
    }
  }
}

/* static */ void
qdb::jni::log::_callback(qdb_log_level_t log_level,
                         const unsigned long * date,
                         unsigned long pid,
                         unsigned long tid,
                         const char * message_buffer,
                         size_t message_size )
{
    message_t x { log_level,
                  { static_cast<int>(date[0]),
                    static_cast<int>(date[1]),
                    static_cast<int>(date[2]),
                    static_cast<int>(date[3]),
                    static_cast<int>(date[4]),
                    static_cast<int>(date[5]) },
                  static_cast<long>(pid),
                  static_cast<long>(tid),
                  std::string(message_buffer, message_size) };
    std::unique_lock guard(buffer_lock);

    printf("acquired lock, pushing into buffer\n");
    fflush(stdout);

    buffer.push_back(x);
}

/* static */ void
qdb::jni::log::flush(qdb::jni::env & env) {

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
    log::_do_flush(env);
  }
}

/* static */ void
qdb::jni::log::_do_flush(qdb::jni::env & env) {

  fprintf(stdout, "flushing %d messages..\n", buffer.size());
  fflush(stdout);

  std::for_each(buffer.begin(), buffer.end(),
                [& env]
                (message_t const & m) {
                  jclass qdbLogger =
                    qdb::jni::introspect::lookup_class(env,
                                                       "net/quasardb/qdb/Logger");
                  jmethodID logID =
                    introspect::lookup_static_method(env,
                                                     qdbLogger,
                                                     "log",
                                                     "(IIIIIIIJJLjava/lang/String;)V");

                  printf("flushing message: %s\n", m.message.c_str());
                  fflush(stdout);

                  env.instance().CallStaticVoidMethod(qdbLogger,
                                                      logID,
                                                      m.level,
                                                      m.timestamp.year,
                                                      m.timestamp.mon,
                                                      m.timestamp.day,
                                                      m.timestamp.hour,
                                                      m.timestamp.min,
                                                      m.timestamp.sec,
                                                      m.pid,
                                                      m.tid,
                                                      env.instance().NewStringUTF(m.message.c_str()));



                });


  printf("clearing all buffers\n");
  fflush(stdout);
  buffer.clear();

}
