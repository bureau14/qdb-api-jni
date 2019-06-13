#include "log.h"
#include "env.h"
#include "string.h"
#include "object.h"
#include <optional>
#include <sstream>
#include <string_view>

static std::string make_log_str(const unsigned long * date,
    unsigned long pid,
    unsigned long tid,
    const char * message_buffer,
    size_t message_size);

void qdb::jni::log_callback(qdb_log_level_t log_level,
    const unsigned long * date,
    unsigned long pid,
    unsigned long tid,
    const char * message_buffer,
    size_t message_size)
{
    static std::optional<jclass> logger = {};
    static std::optional<jmethodID> debugID = {};
    static std::optional<jmethodID> errorID = {};
    static std::optional<jmethodID> fatalID = {};
    static std::optional<jmethodID> infoID = {};
    static std::optional<jmethodID> warnID = {};

    auto env = qdb::jni::env();

    if (!logger)
    {
        logger = introspect::lookup_class(env, "net/quasardb/qdb/QdbCallbackLogger");
        debugID = introspect::lookup_method(env, logger, "debug", "(Ljava/lang/String;)V");
        errorID = introspect::lookup_method(env, logger, "error", "(Ljava/lang/String;)V");
        fatalID = introspect::lookup_method(env, logger, "fatal", "(Ljava/lang/String;)V");
        infoID = introspect::lookup_method(env, logger, "info", "(Ljava/lang/String;)V");
        warnID = introspect::lookup_method(env, logger, "warn", "(Ljava/lang/String;)V");
    }

    auto to_log = make_log_str(date, pid, tid, message_buffer, message_size);
    auto msg = qdb::jni::string::create_utf8(env, to_log.c_str());

    switch (log_level)
    {
    case qdb_log_detailed:
        // TODO(vianney): create a custom logger for this?
        qdb::jni::object::call_static_method(env, *logger, *debugID, msg);
        break;
    case qdb_log_debug:
        qdb::jni::object::call_static_method(env, *logger, *debugID, msg);
        break;
    case qdb_log_info:
        qdb::jni::object::call_static_method(env, *logger, *infoID, msg);
        break;
    case qdb_log_warning:
        qdb::jni::object::call_static_method(env, *logger, *warnID, msg);
        break;
    case qdb_log_error:
        qdb::jni::object::call_static_method(env, *logger, *errorID, msg);
        break;
    case qdb_log_panic:
        qdb::jni::object::call_static_method(env, *logger, *fatalID, msg);
        break;
    }
}

static std::string make_log_str(const unsigned long * date,
    unsigned long pid,
    unsigned long tid,
    const char * message_buffer,
    size_t message_size)
{
    constexpr size_t prefix_size = 30;

    size_t msg_size = prefix_size + message_size;
    std::string msg;
    msg.resize(msg_size);

    auto count = snprintf(msg.c_str(), msg_size, "%02lu/%02lu/%04lu-%02lu:%02lu:%02lu (%5lu:%5lu): %.*s\n", date[1], date[2], date[0], date[3], date[4], date[5], pid, tid, (int)message_size, message_buffer);
    msg.resize(count);
    return msg;
}
