#include "ts_helpers.h"

#include "../debug.h"
#include "../object.h"
#include "../string.h"
#include "../introspect.h"
#include "../env.h"
#include "../local_frame.h"

#include <cassert>
#include <cstring>
#include <cstdlib>

namespace jni = qdb::jni;

qdb_ts_column_type_t
columnTypeFromTypeEnum(qdb::jni::env & env, jobject typeObject) {
  jclass objectClass;
  jfieldID typeValueField;

  objectClass = env.instance().GetObjectClass(typeObject);
  assert(objectClass != NULL);
  typeValueField = env.instance().GetFieldID(objectClass, "value", "I");

  return (qdb_ts_column_type_t)(env.instance().GetIntField(typeObject, typeValueField));
}

qdb_ts_column_type_t
columnTypeFromColumnValue(qdb::jni::env & env, jobject value) {
  jclass objectClass;
  jobject typeObject;
  // jfieldID typeField;
  jfieldID typeValueField;
  jmethodID methodId;

  objectClass = env.instance().GetObjectClass(value);

  // First get the
  methodId = env.instance().GetMethodID(objectClass, "getType", "()Lnet/quasardb/qdb/ts/Value$Type;");
  assert(methodId != NULL);

  return columnTypeFromTypeEnum(env, env.instance().CallObjectMethod(value, methodId));
}

void
timespecToNative(qdb::jni::env & env, jobject input, qdb_timespec_t * output) {
  // qdb_timespec -> tv_sec, tv_nsec
  jfieldID sec_field, nsec_field;
  jclass object_class;

  object_class = env.instance().GetObjectClass(input);

  sec_field = env.instance().GetFieldID(object_class, "sec", "J");
  nsec_field = env.instance().GetFieldID(object_class, "nsec", "J");

  output->tv_sec = env.instance().GetLongField(input, sec_field);
  output->tv_nsec = env.instance().GetLongField(input, nsec_field);
}

jni::guard::local_ref<jobject>
nativeToTimespec(qdb::jni::env & env, qdb_timespec_t input) {
    return std::move(
        jni::object::create(env,
                            "net/quasardb/qdb/ts/Timespec",
                            "(JJ)V",
                            input.tv_sec,
                            input.tv_nsec));
}

void
timeRangeToNative(qdb::jni::env & env, jobject input, qdb_ts_range_t * native) {
  jfieldID beginField, endField;
  jclass objectClass;

  objectClass = env.instance().GetObjectClass(input);

  beginField = env.instance().GetFieldID(objectClass, "begin", "Lnet/quasardb/qdb/ts/Timespec;");
  endField = env.instance().GetFieldID(objectClass, "end", "Lnet/quasardb/qdb/ts/Timespec;");

  timespecToNative(env, env.instance().GetObjectField(input, beginField), &(native->begin));
  timespecToNative(env, env.instance().GetObjectField(input, endField), &(native->end));
}

void
timeRangesToNative(qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_range_t * native) {

  qdb_ts_range_t * cur = native;
  for (size_t i = 0; i < count; ++i) {
      jobject point =
          (jobject)(env.instance().GetObjectArrayElement(input, static_cast<jsize>(i)));

      timeRangeToNative(env, point, cur++);
  }
}

jni::guard::local_ref<jobject>
nativeToTimeRange(qdb::jni::env & env, qdb_ts_range_t native) {
  return std::move(
      jni::object::create(env,
                          "net/quasardb/qdb/ts/TimeRange",
                          "(Lnet/quasardb/qdb/ts/Timespec;Lnet/quasardb/qdb/ts/Timespec;)V",
                          nativeToTimespec(env, native.begin).release(),
                          nativeToTimespec(env, native.end).release()));
}

void
columnsToNative(qdb::jni::env & env, jobjectArray columns, qdb_ts_column_info_t * native_columns, size_t column_count) {
  jfieldID nameField, typeField;
  jclass objectClass;
  for (size_t i = 0; i < column_count; ++i) {
      jobject object =
          (jobject)(env.instance().GetObjectArrayElement(columns, static_cast<jsize>(i)));

      objectClass = env.instance().GetObjectClass(object);
      nameField = env.instance().GetFieldID(objectClass, "name", "Ljava/lang/String;");
      typeField = env.instance().GetFieldID(objectClass, "type", "Lnet/quasardb/qdb/ts/Value$Type;");

      jstring name = (jstring)env.instance().GetObjectField(object, nameField);

      native_columns[i].type =
        columnTypeFromTypeEnum(env, env.instance().GetObjectField(object, typeField));

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4996) // 'strdup': The POSIX name for this item is deprecated. Instead, use the ISO C and C++ conformant name: _strdup.
#endif
      // Is there a better way to do this? Because we're using strdup here, we
      // need a separate release function which is fragile.
      native_columns[i].name = strdup(qdb::jni::string::get_chars_utf8(env, name));
#ifdef _MSC_VER
#pragma warning(pop)
#endif
  }
}

void
releaseNative(qdb_ts_column_info_t * native_columns, size_t column_count) {
  for (size_t i = 0; i < column_count; ++i) {
    free((void *)(native_columns[i].name));
  }
}

jni::guard::local_ref<jobjectArray>
nativeToColumns(qdb::jni::env & env, qdb_ts_column_info_t * nativeColumns, size_t column_count) {
    jni::guard::local_ref<jobjectArray> columns = jni::object::create_array(env,
                                                                            column_count,
                                                                            "net/quasardb/qdb/ts/Column");
    for (size_t i = 0; i < column_count; i++) {
        env.instance().SetObjectArrayElement(
            columns,
            (jsize)i,
            jni::object::create(env,
                                "net/quasardb/qdb/ts/Column",
                                "(Ljava/lang/String;I)V",
                                jni::string::create_utf8(env,
                                                         nativeColumns[i].name).get(),
                                nativeColumns[i].type).release());
    }

    return std::move(columns);
}

void
doublePointToNative(qdb::jni::env & env, jobject input, qdb_ts_double_point * native) {
  jfieldID timestampField, valueField;
  jclass objectClass;

  objectClass = env.instance().GetObjectClass(input);

  timestampField = env.instance().GetFieldID(objectClass, "timestamp", "Lnet/quasardb/qdb/ts/Timespec;");
  valueField = env.instance().GetFieldID(objectClass, "value", "D");

  timespecToNative(env, env.instance().GetObjectField(input, timestampField), &(native->timestamp));
  native->value = env.instance().GetDoubleField(input, valueField);
}

void
doublePointsToNative(qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_double_point * native) {
  qdb_ts_double_point * cur = native;
  for (size_t i = 0; i < count; ++i) {
      jobject point =
          (jobject)(env.instance().GetObjectArrayElement(input, static_cast<jsize>(i)));

      doublePointToNative(env, point, cur++);
  }
}

jni::guard::local_ref<jobject>
nativeToDoublePoint(qdb::jni::env & env, qdb_ts_double_point native) {
  return std::move(
      jni::object::create(env,
                          "net/quasardb/qdb/jni/qdb_ts_double_point",
                          "(Lnet/quasardb/qdb/ts/Timespec;D)V",
                          nativeToTimespec(env, native.timestamp).release(),
                          native.value));
}

jni::guard::local_ref<jobjectArray>
nativeToDoublePoints(qdb::jni::env & env, qdb_ts_double_point * native, size_t count) {
    jni::guard::local_ref<jobjectArray> output(
      jni::object::create_array(env,
                                count,
                                "net/quasardb/qdb/jni/qdb_ts_double_point"));

  for (size_t i = 0; i < count; i++) {
    env.instance().SetObjectArrayElement(output, (jsize)i,
                                         nativeToDoublePoint(env, native[i]).release());
  }

  return std::move(output);
}

void
blobPointToNative(qdb::jni::env & env, jobject input, qdb_ts_blob_point * native) {
  jfieldID timestampField, valueField;
  jclass objectClass;
  jobject value;

  objectClass = env.instance().GetObjectClass(input);

  timestampField = env.instance().GetFieldID(objectClass, "timestamp", "Lnet/quasardb/qdb/ts/Timespec;");
  valueField = env.instance().GetFieldID(objectClass, "value", "Ljava/nio/ByteBuffer;");
  value = env.instance().GetObjectField(input, valueField);

  timespecToNative(env, env.instance().GetObjectField(input, timestampField), &(native->timestamp));
  native->content = env.instance().GetDirectBufferAddress(value);
  native->content_length = (qdb_size_t)env.instance().GetDirectBufferCapacity(value);
}

void
blobPointsToNative(qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_blob_point * native) {
  qdb_ts_blob_point * cur = native;
  for (size_t i = 0; i < count; ++i) {
      jobject point =
          (jobject)(env.instance().GetObjectArrayElement(input, static_cast<jsize>(i)));

      blobPointToNative(env, point, cur++);
  }
}

jobject
nativeToByteBuffer(qdb::jni::env & env, void const * content, qdb_size_t contentLength) {
  return env.instance().NewDirectByteBuffer((void *)(content), contentLength);
}

jni::guard::local_ref<jobject>
nativeToBlobPoint(qdb::jni::env & env, qdb_ts_blob_point native) {
    return std::move(
        jni::object::create(env,
                            "net/quasardb/qdb/jni/qdb_ts_blob_point",
                            "(Lnet/quasardb/qdb/ts/Timespec;Ljava/nio/ByteBuffer;)V",
                            nativeToTimespec(env,
                                             native.timestamp).release(),
                            nativeToByteBuffer(env,
                                               native.content,
                                               native.content_length)));
}

jni::guard::local_ref<jobjectArray>
nativeToBlobPoints(qdb::jni::env & env, qdb_ts_blob_point * native, size_t count) {
    jni::guard::local_ref<jobjectArray> array(
        jni::object::create_array(env,
                                  count,
                                  "net/quasardb/qdb/jni/qdb_ts_blob_point"));

  for (size_t i = 0; i < count; i++) {
    env.instance().SetObjectArrayElement(array,
                                         (jsize)i,
                                         nativeToBlobPoint(env, native[i]).release());
  }

  return array;
}

void
doubleAggregateToNative(qdb::jni::env & env, jobject input, qdb_ts_double_aggregation_t * native) {
  assert(input != NULL);

  jfieldID typeField, timeRangeField, countField, resultField;
  jclass objectClass;

  objectClass = env.instance().GetObjectClass(input);
  typeField = env.instance().GetFieldID(objectClass, "aggregation_type", "J");
  timeRangeField = env.instance().GetFieldID(objectClass, "time_range", "Lnet/quasardb/qdb/ts/TimeRange;");
  countField = env.instance().GetFieldID(objectClass, "count", "J");
  resultField = env.instance().GetFieldID(objectClass, "result", "Lnet/quasardb/qdb/jni/qdb_ts_double_point;");

  timeRangeToNative(env, env.instance().GetObjectField(input, timeRangeField), &(native->range));
  doublePointToNative(env, env.instance().GetObjectField(input, resultField), &(native->result));

  native->type = static_cast<qdb_ts_aggregation_type_t>(
      env.instance().GetLongField(input, typeField));
  native->count = static_cast<qdb_size_t>(env.instance().GetLongField(input, countField));
}

void
doubleAggregatesToNative(qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_double_aggregation_t * native) {
  assert(input != NULL);

  qdb_ts_double_aggregation_t * cur = native;
  for (size_t i = 0; i < count; ++i) {
      jobject aggregate =
          (jobject)(env.instance().GetObjectArrayElement(input, static_cast<jsize>(i)));

      doubleAggregateToNative(env, aggregate, cur++);
  }
}

jni::guard::local_ref<jobject>
nativeToDoubleAggregate(qdb::jni::env & env, qdb_ts_double_aggregation_t native) {
    return std::move(
        jni::object::create(env,
                            "net/quasardb/qdb/jni/qdb_ts_double_aggregation",
                            "(Lnet/quasardb/qdb/ts/TimeRange;JJLnet/quasardb/qdb/jni/qdb_ts_double_point;)V",
                            nativeToTimeRange(env, native.range).release(),
                            (jlong)native.type,
                            (jlong)native.count,
                            nativeToDoublePoint(env, native.result).release()));
}

jni::guard::local_ref<jobjectArray>
nativeToDoubleAggregates(qdb::jni::env & env, qdb_ts_double_aggregation_t * native, size_t count) {
  jni::guard::local_ref<jobjectArray> output (
      jni::object::create_array(env,
                                count,
                                "net/quasardb/qdb/jni/qdb_ts_double_aggregation"));

  for (size_t i = 0; i < count; i++) {
    env.instance().SetObjectArrayElement(output,
                                         (jsize)i,
                                         nativeToDoubleAggregate(env, native[i]).release());
  }

  return std::move(output);
}

void
blobAggregateToNative(qdb::jni::env & env, jobject input, qdb_ts_blob_aggregation_t * native) {
  assert(input != NULL);

  jfieldID typeField, timeRangeField, countField, resultField;
  jclass objectClass;

  objectClass = env.instance().GetObjectClass(input);
  typeField = env.instance().GetFieldID(objectClass, "aggregation_type", "J");
  timeRangeField = env.instance().GetFieldID(objectClass, "time_range", "Lnet/quasardb/qdb/ts/TimeRange;");
  countField = env.instance().GetFieldID(objectClass, "count", "J");
  resultField = env.instance().GetFieldID(objectClass, "result", "Lnet/quasardb/qdb/jni/qdb_ts_blob_point;");

  timeRangeToNative(env, env.instance().GetObjectField(input, timeRangeField), &(native->range));
  blobPointToNative(env, env.instance().GetObjectField(input, resultField), &(native->result));

  native->type = static_cast<qdb_ts_aggregation_type_t>(
      env.instance().GetLongField(input, typeField));
  native->count = static_cast<qdb_size_t>(env.instance().GetLongField(input, countField));
}

void
blobAggregatesToNative(qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_blob_aggregation_t * native) {
    assert(input != NULL);

    qdb_ts_blob_aggregation_t * cur = native;
    for (size_t i = 0; i < count; ++i) {
        jobject aggregate =
            (jobject)(env.instance().GetObjectArrayElement(input, static_cast<jsize>(i)));

        blobAggregateToNative(env, aggregate, cur++);
    }
}

jni::guard::local_ref<jobject>
nativeToBlobAggregate(qdb::jni::env & env, qdb_ts_blob_aggregation_t native) {
    return std::move(
        jni::object::create(env,
                            "net/quasardb/qdb/jni/qdb_ts_blob_aggregation",
                            "(Lnet/quasardb/qdb/ts/ange;JJLnet/quasardb/qdb/jni/qdb_ts_blob_point;)V",
                            nativeToTimeRange(env, native.range).release(),
                            (jlong)native.type,
                            (jlong)native.count,
                            nativeToBlobPoint(env, native.result).release()));
}

jni::guard::local_ref<jobjectArray>
nativeToBlobAggregates(qdb::jni::env & env, qdb_ts_blob_aggregation_t * native, size_t count) {

  jni::guard::local_ref<jobjectArray>
    output(jni::object::create_array(env,
                                     count,
                                     "net/quasardb/qdb/jni/qdb_ts_blob_aggregation"));

    for (size_t i = 0; i < count; i++) {
      env.instance().SetObjectArrayElement(output,
                                           (jsize)i,
                                           nativeToBlobAggregate(env, native[i]).release());
    }

    return std::move(output);
}

void
printObjectClass(qdb::jni::env & env, jobject value) {
  jclass cls = env.instance().GetObjectClass(value);

  // First get the class object
  jmethodID mid = env.instance().GetMethodID(cls, "getClass", "()Ljava/lang/Class;");
  jobject clsObj = env.instance().CallObjectMethod(value, mid);

  // Now get the class object's class descriptor
  cls = env.instance().GetObjectClass(clsObj);

  // Find the getName() method on the class object
  mid = env.instance().GetMethodID(cls, "getName", "()Ljava/lang/String;");

  // Call the getName() to get a jstring object back
  jstring strObj = (jstring)env.instance().CallObjectMethod(clsObj, mid);

  // Now get the c string from the java jstring object
  const char* str = env.instance().GetStringUTFChars(strObj, NULL);

  // Print the class name
  printf("\nCalling class is: %s\n", str);

  // Release the memory pinned char array
  env.instance().ReleaseStringUTFChars(strObj, str);
}

qdb_error_t
tableRowSetInt64ColumnValue(qdb::jni::env & env, qdb_local_table_t localTable, size_t columnIndex, jobject value) {
  jclass objectClass = env.instance().GetObjectClass(value);
  jmethodID methodId = env.instance().GetMethodID(objectClass, "getInt64", "()J");
  assert(methodId != NULL);

  return qdb_ts_row_set_int64(localTable, columnIndex, env.instance().CallLongMethod(value, methodId));
}

qdb_error_t
tableRowSetDoubleColumnValue(qdb::jni::env & env, qdb_local_table_t localTable, size_t columnIndex, jobject value) {
  jclass objectClass = env.instance().GetObjectClass(value);
  jmethodID methodId = env.instance().GetMethodID(objectClass, "getDouble", "()D");
  assert(methodId != NULL);

  return qdb_ts_row_set_double(localTable, columnIndex, env.instance().CallDoubleMethod(value, methodId));
}

qdb_error_t
tableRowSetTimestampColumnValue(qdb::jni::env & env, qdb_local_table_t localTable, size_t columnIndex, jobject value) {
  jclass objectClass = env.instance().GetObjectClass(value);
  jmethodID methodId = env.instance().GetMethodID(objectClass, "getTimestamp", "()Lnet/quasardb/qdb/ts/Timespec;");
  assert(methodId != NULL);

  jobject timestampObject = env.instance().CallObjectMethod(value, methodId);
  assert(timestampObject != NULL);

  qdb_timespec_t timestamp;
  timespecToNative(env, timestampObject, &timestamp);

  qdb_error_t err = qdb_ts_row_set_timestamp(localTable, columnIndex, &timestamp);
  env.instance().DeleteLocalRef(timestampObject);
  return err;
}

qdb_error_t
tableRowSetBlobColumnValue(qdb::jni::env & env, qdb_local_table_t localTable, size_t columnIndex, jobject value) {
  jclass objectClass = env.instance().GetObjectClass(value);
  jmethodID methodId = env.instance().GetMethodID(objectClass, "getBlob", "()Ljava/nio/ByteBuffer;");
  assert(methodId != NULL);

  jobject blobValue = env.instance().CallObjectMethod(value, methodId);

  void * blob_addr = env.instance().GetDirectBufferAddress(blobValue);
  qdb_size_t blob_size = (qdb_size_t)(env.instance().GetDirectBufferCapacity(blobValue));

  qdb_error_t err =  qdb_ts_row_set_blob(localTable,
                                         columnIndex,
                                         blob_addr,
                                         blob_size);
  env.instance().DeleteLocalRef(blobValue);
  return err;
}



qdb_error_t
tableRowSetColumnValue(qdb::jni::env & env, qdb_local_table_t localTable, size_t columnIndex, jobject value) {
  // jclass objectClass;
  // jobject typeObject;
  // jfieldID typeField;
  // jmethodID methodId;

  qdb_ts_column_type_t type = columnTypeFromColumnValue(env, value);

  switch(type) {
  case qdb_ts_column_int64:
    return tableRowSetInt64ColumnValue(env, localTable, columnIndex, value);
    break;

  case qdb_ts_column_double:
    return tableRowSetDoubleColumnValue(env, localTable, columnIndex, value);
    break;

  case qdb_ts_column_timestamp:
    return tableRowSetTimestampColumnValue(env, localTable, columnIndex, value);
    break;

  case qdb_ts_column_blob:
    return tableRowSetBlobColumnValue(env, localTable, columnIndex, value);
    break;

  case qdb_ts_column_uninitialized:
    return qdb_e_ok;
    break;

  default:
    return qdb_e_incompatible_type;
  }
}

qdb_error_t
tableRowAppend(qdb::jni::env & env, qdb_local_table_t localTable, jobject time, jobjectArray values, size_t count, qdb_size_t * rowIndex) {

    jni::local_frame lf = jni::local_frame::push(env,
                                                 // We need at least 2 * count local frame registers
                                                 // from the JVM, because some tableRowSetColumnValue
                                                 // requires additional object references.
                                                 (2 * count));

    qdb_timespec_t nativeTime;
    timespecToNative(env, time, &nativeTime);

    for (size_t i = 0; i < count; i++) {
        jobject value =
            (jobject)(env.instance().GetObjectArrayElement(values, static_cast<jsize>(i)));
        qdb_error_t err = tableRowSetColumnValue(env, localTable, i, value);

        if (!QDB_SUCCESS(err)) {
            return err;
        }
    }

    return qdb_ts_table_row_append(localTable, &nativeTime, rowIndex);
}


qdb_error_t
tableGetRanges(qdb::jni::env & env, qdb_local_table_t localTable, jobjectArray ranges) {
  qdb_size_t rangesCount = env.instance().GetArrayLength(ranges);
  qdb_ts_range_t * nativeRanges =
    (qdb_ts_range_t *)(malloc(rangesCount * sizeof(qdb_ts_range_t)));

  rangesToNative(env, ranges, rangesCount, nativeRanges);

  qdb_error_t err = qdb_ts_table_get_ranges(localTable, nativeRanges, rangesCount);

  free(nativeRanges);
  return err;
}

qdb_error_t
tableGetRowInt64Value(qdb::jni::env & env, qdb_local_table_t localTable, qdb_size_t index, jobject output) {
  qdb_int_t value;
  qdb_error_t err = qdb_ts_row_get_int64(localTable, index, &value);

  if (QDB_SUCCESS(err)) {
    jclass objectClass = env.instance().GetObjectClass(output);
    jmethodID methodId = env.instance().GetMethodID(objectClass, "setInt64", "(J)V");
    assert(methodId != NULL);

    env.instance().CallVoidMethod(output, methodId, static_cast<jlong>(value));
  }

  return err;
}

void
tableGetRowNullValue(qdb::jni::env & env, qdb_local_table_t localTable, qdb_size_t index, jobject output) {
  jclass objectClass = env.instance().GetObjectClass(output);
  jmethodID methodId = env.instance().GetMethodID(objectClass, "setNull", "()V");
  assert(methodId != NULL);

  env.instance().CallVoidMethod(output, methodId);
}

qdb_error_t
tableGetRowDoubleValue(qdb::jni::env & env, qdb_local_table_t localTable, qdb_size_t index, jobject output) {
  double value;
  qdb_error_t err = qdb_ts_row_get_double(localTable, index, &value);

  if (QDB_SUCCESS(err)) {
    jclass objectClass = env.instance().GetObjectClass(output);
    jmethodID methodId = env.instance().GetMethodID(objectClass, "setDouble", "(D)V");
    assert(methodId != NULL);

    env.instance().CallVoidMethod(output, methodId, value);
  }

  return err;
}

qdb_error_t
tableGetRowTimestampValue(qdb::jni::env & env, qdb_local_table_t localTable, qdb_size_t index, jobject output) {

  qdb_timespec_t value;

  qdb_error_t err = qdb_ts_row_get_timestamp(localTable, index, &value);

  if (QDB_SUCCESS(err)) {
    jclass objectClass = env.instance().GetObjectClass(output);
    jmethodID methodId = env.instance().GetMethodID(objectClass, "setTimestamp", "(Lnet/quasardb/qdb/ts/Timespec;)V");
    assert(methodId != NULL);

    env.instance().CallVoidMethod(output, methodId, nativeToTimespec(env, value).release());
  }

  return err;
}

qdb_error_t
tableGetRowBlobValue(qdb::jni::env & env, qdb_local_table_t localTable, qdb_size_t index, jobject output) {

  void const * value = NULL;
  qdb_size_t length = 0;

  qdb_error_t err = qdb_ts_row_get_blob(localTable, index, &value, &length);

  if (QDB_SUCCESS(err)) {
    assert(value != NULL);

    jobject byteBuffer = nativeToByteBuffer(env, value, length);
    assert(byteBuffer != NULL);

    jclass objectClass = env.instance().GetObjectClass(output);
    jmethodID methodId = env.instance().GetMethodID(objectClass, "setBlob", "(Ljava/nio/ByteBuffer;)V");
    assert(methodId != NULL);

    env.instance().CallVoidMethod(output, methodId, byteBuffer);
  }

  return err;
}

qdb_error_t
tableGetRowValues (qdb::jni::env & env, qdb_local_table_t localTable, qdb_ts_column_info_t * columns, qdb_size_t count, jobjectArray values) {
  qdb_error_t err;

  jclass valueClass = env.instance().FindClass("net/quasardb/qdb/ts/Value");
  assert(valueClass != NULL);
  jmethodID constructor = env.instance().GetStaticMethodID(valueClass, "createNull", "()Lnet/quasardb/qdb/ts/Value;");
  assert(constructor != NULL);


  for (size_t i = 0; i < count; ++i) {
    qdb_ts_column_info_t column = columns[i];

    jobject value = env.instance().CallStaticObjectMethod(valueClass, constructor);
    switch (column.type) {
    case qdb_ts_column_double:
      err = tableGetRowDoubleValue(env, localTable, i, value);
      break;

    case qdb_ts_column_int64:
      err = tableGetRowInt64Value(env, localTable, i, value);
      break;

    case qdb_ts_column_timestamp:
      err = tableGetRowTimestampValue(env, localTable, i, value);
      break;

    case qdb_ts_column_blob:
      err = tableGetRowBlobValue(env, localTable, i, value);
      break;

    case qdb_ts_column_uninitialized:
      tableGetRowNullValue(env, localTable, i, value);
      break;

    default:
      err = qdb_e_incompatible_type;
      break;
    }

    if(QDB_FAILURE(err)) {
      return err;
    }

    env.instance().SetObjectArrayElement(values, (jsize)i, value);
  }

  return qdb_e_ok;
}

qdb_error_t
tableGetRow(qdb::jni::env & env, qdb_local_table_t localTable, qdb_ts_column_info_t * columns, qdb_size_t columnCount, jobject * output) {

  qdb_timespec_t timestamp;
  qdb_error_t err = qdb_ts_table_next_row(localTable, &timestamp);

  if (err == qdb_e_iterator_end) {
    return err;
  }

  if (QDB_SUCCESS(err)) {
    jni::guard::local_ref<jobjectArray> values = jni::object::create_array(env,
                                                                           columnCount,
                                                                           "net/quasardb/qdb/ts/Value");

    err = tableGetRowValues(env, localTable, columns, columnCount, values);

    if (QDB_SUCCESS(err)) {
        *output =
            jni::object::create(env,
                                "net/quasardb/qdb/ts/Row",
                                "(Lnet/quasardb/qdb/ts/Timespec;[Lnet/quasardb/qdb/ts/Value;)V",
                                nativeToTimespec(env, timestamp).release(),
                                values.release()).release();
    }
  }

  return err;
}
