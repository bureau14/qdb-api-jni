#include "helpers.h"
#include "../byte_buffer.h"
#include "../env.h"
#include "../exception.h"
#include "../log.h"

void setReferenceValue(qdb::jni::env & env, jobject reference, jobject value)
{
    static jfieldID fid = 0;
    if (!fid)
    {
        jclass thisClass = env.instance().GetObjectClass(reference);
        fid              = env.instance().GetFieldID(thisClass, "value", "Ljava/lang/Object;");
    }
    env.instance().SetObjectField(reference, fid, value);
}

jobject getReferenceValue(qdb::jni::env & env, jobject reference)
{
    static jfieldID fid = 0;
    if (!fid)
    {
        jclass thisClass = env.instance().GetObjectClass(reference);
        fid              = env.instance().GetFieldID(thisClass, "value", "Ljava/lang/Object;");
    }

    return env.instance().GetObjectField(reference, fid);
}

void setByteBuffer(
    qdb::jni::env & env, qdb_handle_t handle, jobject reference, const void * ptr, jsize size)
{
    setReferenceValue(env, reference,
        ptr ? qdb::jni::byte_buffer::create_copy(env, handle, ptr, size).get() : NULL);
}

void setInteger(qdb::jni::env & env, jobject reference, jint value)
{
    jclass integerClass            = env.instance().FindClass("java/lang/Integer");
    static jmethodID constructorId = 0;
    if (!constructorId)
    {
        constructorId = env.instance().GetMethodID(integerClass, "<init>", "(I)V");
    }
    setReferenceValue(env, reference, env.instance().NewObject(integerClass, constructorId, value));
}

void setLong(qdb::jni::env & env, jobject reference, jlong value)
{
    jclass longClass               = env.instance().FindClass("java/lang/Long");
    static jmethodID constructorId = 0;
    if (!constructorId)
    {
        constructorId = env.instance().GetMethodID(longClass, "<init>", "(J)V");
    }
    setReferenceValue(env, reference, env.instance().NewObject(longClass, constructorId, value));
}

void setStringArray(qdb::jni::env & env, jobject reference, const char ** strings, size_t count)
{
    jclass stringClass = env.instance().FindClass("java/lang/String");
    jobjectArray array = env.instance().NewObjectArray((jsize)count, stringClass, NULL);
    for (size_t i = 0; i < count; i++)
    {
        env.instance().SetObjectArrayElement(
            array, (jsize)i, env.instance().NewStringUTF(strings[i]));
    }
    setReferenceValue(env, reference, array);
}

void setString(qdb::jni::env & env, jobject reference, const char * value)
{
    setReferenceValue(env, reference, env.instance().NewStringUTF(value));
}
