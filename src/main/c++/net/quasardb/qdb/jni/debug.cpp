#include "debug.h"
#include "env.h"
#include "introspect.h"

/* static */ void
qdb::jni::debug::hexdump(env &env, void const *buf_, size_t len)
{
    char const *buf = (char const *)(buf_);
    char const *const lut = "0123456789ABCDEF";

    std::string output;
    output.reserve(3 * len);
    output.append("* NATIVE * ");

    for (size_t i = 0; i < len; ++i)
    {
        const unsigned char c = buf[i];
        output.push_back(lut[c >> 4]);
        output.push_back(lut[c & 15]);
        output.push_back(' ');
    }

    println(env, output);
}

/* static */ void
qdb::jni::debug::println(env &env, std::string const &msg)
{
    println(env, msg.c_str());
}

/* static */ void
qdb::jni::debug::println(env &env, char const *msg)
{
    jclass syscls = introspect::lookup_class(env, "java/lang/System");

    // Lookup the "out" field
    jfieldID fid = introspect::lookup_static_field(env, syscls, "out",
                                                   "Ljava/io/PrintStream;");
    jobject out = env.instance().GetStaticObjectField(syscls, fid);

    // Get PrintStream class
    jclass pscls = introspect::lookup_class(env, "java/io/PrintStream");

    // Lookup printLn(String)
    jmethodID mid = introspect::lookup_method(env, pscls, "println",
                                              "(Ljava/lang/String;)V");

    // Invoke the method
    jstring str = env.instance().NewStringUTF(msg);
    env.instance().CallVoidMethod(out, mid, str);
    env.instance().DeleteLocalRef(str);
}
