#pragma once

#include <qdb/ts.h>
#include <string>

namespace qdb::jni
{

class env;

class debug
{
public:
    static void println(env & env, std::string const & msg);

    static void println(env & env, char const * msg);

    static std::string hexdump(void const * buf, size_t len);
    static std::string hexdump(qdb_blob_t x)
    {
        return hexdump(x.content, x.content_length);
    }

private:
};
}; // namespace qdb::jni
