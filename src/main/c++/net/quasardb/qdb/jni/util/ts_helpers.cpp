#include "ts_helpers.h"
#include "../adapt/column.h"
#include "../adapt/timerange.h"
#include "../adapt/timespec.h"
#include "../byte_buffer.h"
#include "../debug.h"
#include "../env.h"
#include "../exception.h"
#include "../introspect.h"
#include "../local_frame.h"
#include "../log.h"
#include "../object.h"
#include "../string.h"
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>

namespace jni = qdb::jni;

void doublePointToNative(qdb::jni::env & env, jobject input, qdb_ts_double_point * native)
{
    jfieldID timestampField, valueField;
    jclass objectClass;

    objectClass = env.instance().GetObjectClass(input);

    timestampField =
        env.instance().GetFieldID(objectClass, "timestamp", "Lnet/quasardb/qdb/ts/Timespec;");
    valueField = env.instance().GetFieldID(objectClass, "value", "D");

    native->timestamp =
        jni::adapt::timespec::to_qdb(env, env.instance().GetObjectField(input, timestampField));

    native->value = env.instance().GetDoubleField(input, valueField);
}

void doublePointsToNative(
    qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_double_point * native)
{
    qdb_ts_double_point * cur = native;
    for (size_t i = 0; i < count; ++i)
    {
        jobject point =
            (jobject)(env.instance().GetObjectArrayElement(input, static_cast<jsize>(i)));

        doublePointToNative(env, point, cur++);
    }
}

jni::guard::local_ref<jobject> nativeToDoublePoint(qdb::jni::env & env, qdb_ts_double_point native)
{
    return jni::object::create(env, "net/quasardb/qdb/jni/qdb_ts_double_point",
        "(Lnet/quasardb/qdb/ts/Timespec;D)V",
        jni::adapt::timespec::to_java(env, native.timestamp).release(), native.value);
}

jni::guard::local_ref<jobjectArray> nativeToDoublePoints(
    qdb::jni::env & env, qdb_ts_double_point * native, size_t count)
{
    jni::guard::local_ref<jobjectArray> output(
        jni::object::create_array(env, count, "net/quasardb/qdb/jni/qdb_ts_double_point"));

    for (size_t i = 0; i < count; i++)
    {
        if (!QDB_IS_NULL_DOUBLE(native[i]))
        {
            env.instance().SetObjectArrayElement(
                output, (jsize)i, nativeToDoublePoint(env, native[i]).release());
        }
    }

    return output;
}

void blobPointToNative(qdb::jni::env & env, jobject input, qdb_ts_blob_point * native)
{
    jfieldID timestampField, valueField;
    jclass objectClass;
    jobject value;

    objectClass = env.instance().GetObjectClass(input);

    timestampField =
        env.instance().GetFieldID(objectClass, "timestamp", "Lnet/quasardb/qdb/ts/Timespec;");
    valueField = env.instance().GetFieldID(objectClass, "value", "Ljava/nio/ByteBuffer;");
    value      = env.instance().GetObjectField(input, valueField);

    native->timestamp =
        jni::adapt::timespec::to_qdb(env, env.instance().GetObjectField(input, timestampField));
    native->content        = env.instance().GetDirectBufferAddress(value);
    native->content_length = (qdb_size_t)env.instance().GetDirectBufferCapacity(value);
}

void blobPointsToNative(
    qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_blob_point * native)
{
    qdb_ts_blob_point * cur = native;
    for (size_t i = 0; i < count; ++i)
    {
        jobject point =
            (jobject)(env.instance().GetObjectArrayElement(input, static_cast<jsize>(i)));

        blobPointToNative(env, point, cur++);
    }
}

void doubleAggregateToNative(
    qdb::jni::env & env, jobject input, qdb_ts_double_aggregation_t * native)
{
    assert(input != NULL);

    jfieldID typeField, timeRangeField, countField, resultField;
    jclass objectClass;

    objectClass = env.instance().GetObjectClass(input);
    typeField   = env.instance().GetFieldID(objectClass, "aggregation_type", "J");
    timeRangeField =
        env.instance().GetFieldID(objectClass, "time_range", "Lnet/quasardb/qdb/ts/TimeRange;");
    countField  = env.instance().GetFieldID(objectClass, "count", "J");
    resultField = env.instance().GetFieldID(
        objectClass, "result", "Lnet/quasardb/qdb/jni/qdb_ts_double_point;");

    native->range = jni::adapt::timerange::to_qdb(
        env, jni::object::from_field<jobject>(env, input, timeRangeField));

    doublePointToNative(env, env.instance().GetObjectField(input, resultField), &(native->result));

    native->type =
        static_cast<qdb_ts_aggregation_type_t>(env.instance().GetLongField(input, typeField));
    native->count = static_cast<qdb_size_t>(env.instance().GetLongField(input, countField));
}

void doubleAggregatesToNative(
    qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_double_aggregation_t * native)
{
    assert(input != NULL);

    qdb_ts_double_aggregation_t * cur = native;
    for (size_t i = 0; i < count; ++i)
    {
        jobject aggregate =
            (jobject)(env.instance().GetObjectArrayElement(input, static_cast<jsize>(i)));

        doubleAggregateToNative(env, aggregate, cur++);
    }
}

jni::guard::local_ref<jobject> nativeToDoubleAggregate(
    qdb::jni::env & env, qdb_ts_double_aggregation_t native)
{
    static_assert(sizeof(jlong) >= sizeof(native.type));
    static_assert(sizeof(jlong) >= sizeof(native.count));

    return jni::object::create(env, "net/quasardb/qdb/jni/qdb_ts_double_aggregation",
        "(Lnet/quasardb/qdb/ts/TimeRange;JJLnet/quasardb/qdb/jni/"
        "qdb_ts_double_point;)V",
        jni::adapt::timerange::to_java(env, native.range).release(), (jlong)native.type,
        (jlong)native.count, nativeToDoublePoint(env, native.result).release());
}

jni::guard::local_ref<jobjectArray> nativeToDoubleAggregates(
    qdb::jni::env & env, qdb_ts_double_aggregation_t * native, size_t count)
{
    jni::guard::local_ref<jobjectArray> output(
        jni::object::create_array(env, count, "net/quasardb/qdb/jni/qdb_ts_double_aggregation"));

    for (size_t i = 0; i < count; i++)
    {
        env.instance().SetObjectArrayElement(
            output, (jsize)i, nativeToDoubleAggregate(env, native[i]).release());
    }

    return output;
}

void blobAggregateToNative(qdb::jni::env & env, jobject input, qdb_ts_blob_aggregation_t * native)
{
    assert(input != NULL);

    jfieldID typeField, timeRangeField, countField, resultField;
    jclass objectClass;

    objectClass = env.instance().GetObjectClass(input);
    typeField   = env.instance().GetFieldID(objectClass, "aggregation_type", "J");
    timeRangeField =
        env.instance().GetFieldID(objectClass, "time_range", "Lnet/quasardb/qdb/ts/TimeRange;");
    countField  = env.instance().GetFieldID(objectClass, "count", "J");
    resultField = env.instance().GetFieldID(
        objectClass, "result", "Lnet/quasardb/qdb/jni/qdb_ts_blob_point;");

    native->range = jni::adapt::timerange::to_qdb(
        env, jni::object::from_field<jobject>(env, input, timeRangeField));

    blobPointToNative(env, env.instance().GetObjectField(input, resultField), &(native->result));

    native->type =
        static_cast<qdb_ts_aggregation_type_t>(env.instance().GetLongField(input, typeField));
    native->count = static_cast<qdb_size_t>(env.instance().GetLongField(input, countField));
}

void blobAggregatesToNative(
    qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_blob_aggregation_t * native)
{
    assert(input != NULL);

    qdb_ts_blob_aggregation_t * cur = native;
    for (size_t i = 0; i < count; ++i)
    {
        jobject aggregate =
            (jobject)(env.instance().GetObjectArrayElement(input, static_cast<jsize>(i)));

        blobAggregateToNative(env, aggregate, cur++);
    }
}

jni::guard::local_ref<jobject> nativeToBlobAggregate(
    qdb::jni::env & env, qdb_ts_blob_aggregation_t native)
{
    static_assert(sizeof(jlong) >= sizeof(native.type));
    static_assert(sizeof(jlong) >= sizeof(native.count));

    return jni::object::create(env, "net/quasardb/qdb/jni/qdb_ts_blob_aggregation",
        "(Lnet/quasardb/qdb/ts/TimeRange;JJLnet/quasardb/qdb/jni/"
        "qdb_ts_blob_point;)V",
        jni::adapt::timerange::to_java(env, native.range).release(), (jlong)native.type,
        (jlong)native.count, nativeToBlobPoint(env, native.result).release());
}

jni::guard::local_ref<jobjectArray> nativeToBlobAggregates(
    qdb::jni::env & env, qdb_ts_blob_aggregation_t * native, size_t count)
{
    jni::guard::local_ref<jobjectArray> output(
        jni::object::create_array(env, count, "net/quasardb/qdb/jni/qdb_ts_blob_aggregation"));

    for (size_t i = 0; i < count; i++)
    {
        env.instance().SetObjectArrayElement(
            output, (jsize)i, nativeToBlobAggregate(env, native[i]).release());
    }

    return output;
}

void printObjectClass(qdb::jni::env & env, jobject value)
{
    jclass cls = env.instance().GetObjectClass(value);

    // First get the class object
    jmethodID mid  = env.instance().GetMethodID(cls, "getClass", "()Ljava/lang/Class;");
    jobject clsObj = env.instance().CallObjectMethod(value, mid);

    // Now get the class object's class descriptor
    cls = env.instance().GetObjectClass(clsObj);

    // Find the getName() method on the class object
    mid = env.instance().GetMethodID(cls, "getName", "()Ljava/lang/String;");

    // Call the getName() to get a jstring object back
    jstring strObj = (jstring)env.instance().CallObjectMethod(clsObj, mid);

    // Now get the c string from the java jstring object
    const char * str = env.instance().GetStringUTFChars(strObj, NULL);

    // Print the class name
    printf("\nCalling class is: %s\n", str);

    // Release the memory pinned char array
    env.instance().ReleaseStringUTFChars(strObj, str);
}
