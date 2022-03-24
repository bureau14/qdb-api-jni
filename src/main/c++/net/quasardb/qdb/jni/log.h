#pragma once

#include <qdb/log.h>
#include <string>
#include <time.h>

namespace qdb
{
namespace jni
{
class env;

namespace log
{

typedef struct
{
    int year;
    int mon;
    int day;
    int hour;
    int min;
    int sec;
} message_time_t;

typedef struct
{
    qdb_log_level_t level;
    message_time_t timestamp;

    int pid;
    int tid;
    std::string message;

} message_t;

/**
 * Useful to ensure logs are flushed when a function returns back from native
 * context to JVM context.
 */
class flush_guard
{
public:
    flush_guard(qdb::jni::env & env)
        : env_(env)
    {}

    ~flush_guard();

private:
    qdb::jni::env & env_;
};

/**
 * Since the JVM processes are not necessarily aligned with what the QuasarDB
 * assumes, we need a mechanism to 'bridge' this:
 *
 *  1. qdb api registers log callbacks per process;
 *  2. the JVM may create new processes/threads at will
 *  3. we need to initialize a new log callback any time a new JavaVM process is
 *     started.
 *  4. but this is not possible to hook.
 *  5. thus, we do some hacking with thread_local in this class to make this
 * work.
 *
 * Additionally, the log_callback is called from non-JVM threads, and we
 * actually need a JavaVM of some sort. However, it's impossible to call a
 * JavaVM unless our current thread is actually managed by this JavaVM, leaving
 * us with two choices:
 *
 * 1. Create and manage a completely separate JavaVM solely for logging
 * 2. Hook a 'flush' call on fuction return of each JNI invocation.
 *
 * 2) has the advantage that it's simple and we're reusing existing JavaVM
 * instances, but has the drawback that if no JNI methods are invoked for a long
 * time, no logs will be flushed/appear.
 *
 * Until we figure out a better solution, we will stick with the second
 * solution, however, since managing a completely standalone JavaVM process will
 * cause a lot of overhead.
 */
void swap_callback();

void flush(qdb::jni::env & env);

void _do_log(qdb_log_level_t log_level,
    const unsigned long * date,
    unsigned long pid,
    unsigned long tid,
    std::string const & msg);

void _callback(                  //
    qdb_log_level_t log_level,   // qdb log level
    const unsigned long * date,  // [years, months, day, hours, minute, seconds]
                                 // (valid only in the context of the callback)
    unsigned long pid,           // process id
    unsigned long tid,           // thread id
    const char * message_buffer, // message buffer (valid only in the context of
                                 // the callback)
    size_t message_size);        // message buffer size
void _current_date(unsigned long * date);

/**
 * Convenience function that is used by trace(), debug(), etc
 */
template <typename... Args>
void _wrap_callback(qdb_log_level_t lvl, Args &&... args)
{
    unsigned long date[6];
    _current_date(date);

    // 2kb max log length
    char buffer[2048];
    snprintf(buffer, sizeof(buffer), std::forward<Args>(args)...);

    _do_log(lvl, date, 0, 0, std::string(buffer));
}

/**
 * Implementation function of flush(), which assumes that locks have been
 * acquired and buffer actually contains logs.
 */
void _do_flush(qdb::jni::env & env);

/**
 * Wraps logger.trace. Asynchronous, as to avoid context switches. Ideally the
 * caller calls flush() just before their function returns back to the JVM.
 */
template <typename... Args>
void trace(Args &&... args)
{
    _wrap_callback(qdb_log_detailed, std::forward<Args>(args)...);
}

/**
 * Wraps logger.debug. Asynchronous, as to avoid context switches. Ideally the
 * caller calls flush() just before their function returns back to the JVM.
 */
template <typename... Args>
void debug(Args &&... args)
{
    _wrap_callback(qdb_log_debug, std::forward<Args>(args)...);
}

/**
 * Wraps logger.info. Asynchronous, as to avoid context switches. Ideally the
 * caller calls flush() just before their function returns back to the JVM.
 */
template <typename... Args>
void info(Args &&... args)
{
    _wrap_callback(qdb_log_info, std::forward<Args>(args)...);
}

/**
 * Wraps logger.warn. Asynchronous, as to avoid context switches. Ideally the
 * caller calls flush() just before their function returns back to the JVM.
 */
template <typename... Args>
void warn(Args &&... args)
{
    _wrap_callback(qdb_log_warning, std::forward<Args>(args)...);
}

/**
 * Wraps logger.error. Asynchronous, as to avoid context switches. Ideally the
 * caller calls flush() just before their function returns back to the JVM.
 */
template <typename... Args>
void error(Args &&... args)
{
    _wrap_callback(qdb_log_error, std::forward<Args>(args)...);
}
} // namespace log
} // namespace jni
} // namespace qdb
