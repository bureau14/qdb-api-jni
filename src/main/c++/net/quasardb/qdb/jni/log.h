#pragma once

#include <string>
#include <qdb/log.h>

namespace qdb {
    namespace jni {

        void log_callback( //
            qdb_log_level_t log_level,    // qdb log level
            const unsigned long * date,   // [years, months, day, hours, minute, seconds] (valid only in the context of the callback)
            unsigned long pid,            // process id
            unsigned long tid,            // thread id
            const char * message_buffer,  // message buffer (valid only in the context of the callback)
            size_t message_size);         // message buffer size

    }
}
