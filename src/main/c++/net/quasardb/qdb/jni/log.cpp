#include "log.h"
#include "env.h"
#include "object.h"
#include "string.h"
#include <algorithm>
#include <atomic>
#include <iostream>
#include <optional>
#include <shared_mutex>
#include <string.h>
#include <time.h>
#include <vector>
#include <mutex>

static std::vector<qdb::jni::log::message_t> buffer;
static std::shared_mutex buffer_lock;
static qdb_log_callback_id local_callback_id;

/* static */ void
qdb::jni::log::swap_callback()
{
    // There may be race conditions in the `local_callback_id` and us adding / removing
    // the lock.
    //
    // As it's unlikely that this specific function is a bottleneck, let's just acquire
    // a unique lock here.
    std::unique_lock unique_guard(buffer_lock);

    qdb_error_t error;

    error = qdb_log_remove_callback(local_callback_id);
    if (error)
    {
        // fprintf(stderr, "unable to remove previous callback: %s (%#x)\n",
        //         qdb_error(error), error);
        // fflush(stderr);
    }

    error = qdb_log_add_callback(_callback, &local_callback_id);
    if (error)
    {
        fprintf(stderr, "unable to add new callback: %s (%#x)\n",
                qdb_error(error), error);
        fflush(stderr);
    }
}

void
qdb::jni::log::_do_log(qdb_log_level_t log_level,
                       const unsigned long *date,
                       unsigned long pid,
                       unsigned long tid,
                       std::string const &msg)
{
    qdb::jni::log::message_t x{
        log_level,
        {static_cast<int>(date[0]), static_cast<int>(date[1]),
         static_cast<int>(date[2]), static_cast<int>(date[3]),
         static_cast<int>(date[4]), static_cast<int>(date[5])},
        static_cast<int>(pid),
        static_cast<int>(tid),
        msg};

    std::unique_lock guard(buffer_lock);
    buffer.push_back(x);
}

/* static */ void
qdb::jni::log::_callback(qdb_log_level_t log_level,
                         const unsigned long *date,
                         unsigned long pid,
                         unsigned long tid,
                         const char *message_buffer,
                         size_t message_size)
{
    _do_log(log_level, date, pid, tid,
            std::string(message_buffer, message_size));
}

void
qdb::jni::log::_current_date(unsigned long *date)
{
    time_t now = time(0);
    struct tm *utc = gmtime(&now); // thread safety?

    date[0] = utc->tm_year + 1900;
    date[1] = utc->tm_mon + 1;
    date[2] = utc->tm_mday;
    date[3] = utc->tm_hour;
    date[4] = utc->tm_min;
    date[5] = utc->tm_sec;
}

qdb::jni::log::flush_guard::~flush_guard()
{
    qdb::jni::log::flush(env_);
}

/* static */ void
qdb::jni::log::flush(qdb::jni::env &env)
{

    // Since this function is invoked a lot, and typically the buffer will be
    // empty, we first get a read lock, and then if (and only if) there is
    // actually more than one log available, we flush the log.
    //
    // There is still a chance of a race condition where multiple threads
    // attempt to acquire the exclusive lock when the buffer is non-empty, but
    // we're optimizing for the 'default' case of an empty buffer here.
    std::shared_lock shared_guard(buffer_lock);

    if (!buffer.empty())
    {
        // Non-atomic upgrade to unique lock, for some reason it seems like the
        // STL does not have a mechanism to upgrade?
        shared_guard.unlock();
        std::unique_lock unique_guard(buffer_lock);
        log::_do_flush(env);
    }
}

/* static */ void
qdb::jni::log::_do_flush(qdb::jni::env &env)
{

    // This is safe because it's guaranteed to be single-threaded as we already
    // have a unique_lock on the buffer.
    static std::optional<jclass> qdbLogger = {};
    static std::optional<jmethodID> logID = {};

    if (!qdbLogger)
    {
        assert(!logID);

        qdbLogger.emplace(
            qdb::jni::introspect::lookup_class(env, "net/quasardb/qdb/Logger"));

        logID.emplace(introspect::lookup_static_method(
            env, *qdbLogger, "log", "(IIIIIIIIILjava/lang/String;)V"));
    }

    // TODO(leon): fold loop into a single function call to reduce java <-> jni
    // context
    //             switching.

    for (auto i = buffer.begin(); i != buffer.end(); ++i)
    {
        message_t const &m = *i;
        jstring s = env.instance().NewStringUTF(m.message.c_str());

        env.instance().CallStaticVoidMethod(*qdbLogger, *logID,

                                            m.level,

                                            m.timestamp.year, m.timestamp.mon,
                                            m.timestamp.day, m.timestamp.hour,
                                            m.timestamp.min, m.timestamp.sec,

                                            m.pid, m.tid, s);
    }

    buffer.clear();
}
