#pragma once

#include <time.h>
#include <string>
#include <qdb/log.h>

namespace qdb {
    namespace jni {
      class env;

      namespace log {

        typedef struct  {
          int year;
          int mon;
          int day;
          int hour;
          int min;
          int sec;
        } message_time_t;

        typedef struct {
          qdb_log_level_t level;
          message_time_t timestamp;

          int pid;
          int tid;
          std::string message;

        } message_t;

        /**
         * Since the JVM processes are not necessarily aligned with what the QuasarDB
         * assumes, we need a mechanism to 'bridge' this:
         *
         *  1. qdb api registers log callbacks per process;
         *  2. the JVM may create new processes/threads at will
         *  3. we need to initialize a new log callback any time a new JavaVM process is
         *     started.
         *  4. but this is not possible to hook.
         *  5. thus, we do some hacking with thread_local in this class to make this work.
         *
         * Additionally, the log_callback is called from non-JVM threads, and we actually
         * need a JavaVM of some sort. However, it's impossible to call a JavaVM unless our
         * current thread is actually managed by this JavaVM, leaving us with two choices:
         *
         * 1. Create and manage a completely separate JavaVM solely for logging
         * 2. Hook a 'flush' call on fuction return of each JNI invocation.
         *
         * 2) has the advantage that it's simple and we're reusing existing JavaVM instances,
         * but has the drawback that if no JNI methods are invoked for a long time, no logs
         * will be flushed/appear.
         *
         * Until we figure out a better solution, we will stick with the second solution, however,
         * since managing a completely standalone JavaVM process will cause a lot of overhead.
         */
        void
        ensure_callback(qdb::jni::env & env);

        void
        flush(qdb::jni::env & env);

        void _callback( //
                       qdb_log_level_t log_level,    // qdb log level
                       const unsigned long * date,   // [years, months, day, hours, minute, seconds] (valid only in the context of the callback)
                       unsigned long pid,            // process id
                       unsigned long tid,            // thread id
                       const char * message_buffer,  // message buffer (valid only in the context of the callback)
                       size_t message_size);         // message buffer size

        /**
         * Implementation function of flush(), which assumes that locks have been
         * acquired and buffer actually contains logs.
         */
        void _do_flush(qdb::jni::env & env);

      }
    }
}
