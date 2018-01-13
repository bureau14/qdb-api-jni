#include "ts_helpers.h"

#include <cassert>
#include <cstring>
#include <cstdlib>

void
timespecToNative(JNIEnv *env, jobject input, qdb_timespec_t * output) {
  // qdb_timespec -> tv_sec, tv_nsec
  jfieldID sec_field, nsec_field;
  jclass object_class;

  object_class = env->GetObjectClass(input);

  sec_field = env->GetFieldID(object_class, "sec", "J");
  nsec_field = env->GetFieldID(object_class, "nsec", "J");

  output->tv_sec = env->GetLongField(input, sec_field);
  output->tv_nsec = env->GetLongField(input, nsec_field);
}

void
nativeToTimespec(JNIEnv *env, qdb_timespec_t input, jobject * output) {
  jclass timespec_class = env->FindClass("net/quasardb/qdb/QdbTimespec");
  assert(timespec_class != NULL);
  jmethodID constructor = env->GetMethodID(timespec_class, "<init>", "(JJ)V");
  assert(constructor != NULL);

  *output = env->NewObject(timespec_class,
                           constructor,
                           input.tv_sec,
                           input.tv_nsec);
}

void
rangeToNative(JNIEnv *env, jobject input, qdb_ts_range_t * native) {
  jfieldID beginField, endField;
  jclass objectClass;

  objectClass = env->GetObjectClass(input);

  beginField = env->GetFieldID(objectClass, "begin", "Lnet/quasardb/qdb/QdbTimespec;");
  endField = env->GetFieldID(objectClass, "end", "Lnet/quasardb/qdb/QdbTimespec;");

  timespecToNative(env, env->GetObjectField(input, beginField), &(native->begin));
  timespecToNative(env, env->GetObjectField(input, endField), &(native->end));
}

void
rangesToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_range_t * native) {
  qdb_ts_range_t * cur = native;
  for (size_t i = 0; i < count; ++i) {
      jobject point =
          (jobject)(env->GetObjectArrayElement(input, static_cast<jsize>(i)));

      rangeToNative(env, point, cur++);
  }
}

void
filterToNative(JNIEnv * /*env*/, jobject /*input*/, qdb_ts_filter_t * native) {
  native->type = qdb_ts_filter_none;
}

void
nativeToFilter(JNIEnv *env, qdb_ts_filter_t input, jobject * output) {
  assert(input.type == qdb_ts_filter_none);

  jclass no_filter_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_no_filter");
  assert(no_filter_class != NULL);
  jmethodID constructor = env->GetMethodID(no_filter_class, "<init>", "()V");
  assert(constructor != NULL);

  *output = env->NewObject(no_filter_class,
                           constructor);
}

void
filteredRangeToNative(JNIEnv *env, jobject input, qdb_ts_filtered_range_t * native) {
  jfieldID rangeField, filterField;
  jclass objectClass;

  objectClass = env->GetObjectClass(input);

  rangeField = env->GetFieldID(objectClass, "range", "Lnet/quasardb/qdb/QdbTimeRange;");
  filterField = env->GetFieldID(objectClass, "filter", "Lnet/quasardb/qdb/jni/qdb_ts_filter;");

  rangeToNative(env, env->GetObjectField(input, rangeField), &(native->range));
  filterToNative(env, env->GetObjectField(input, filterField), &(native->filter));
}

void
filteredRangesToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_filtered_range_t * native) {
  qdb_ts_filtered_range_t * cur = native;

  for (size_t i = 0; i < count; ++i) {
      jobject point =
          (jobject)(env->GetObjectArrayElement(input, static_cast<jsize>(i)));

      filteredRangeToNative(env, point, cur++);
  }
}

void
nativeToRange(JNIEnv * env, qdb_ts_range_t native, jobject * output) {
  jclass point_class = env->FindClass("net/quasardb/qdb/QdbTimeRange");
  assert(point_class != NULL);
  jmethodID constructor = env->GetMethodID(point_class, "<init>", "(Lnet/quasardb/qdb/QdbTimespec;Lnet/quasardb/qdb/QdbTimespec;)V");
  assert(constructor != NULL);

  jobject begin;
  jobject end;

  nativeToTimespec(env, native.begin, &begin);
  nativeToTimespec(env, native.end, &end);

  *output = env->NewObject(point_class,
                           constructor,
                           begin,
                           end);
}

void
nativeToFilteredRange(JNIEnv * env, qdb_ts_filtered_range_t native, jobject * output) {
  jclass filtered_range_class = env->FindClass("net/quasardb/qdb/QdbFilteredRange");
  assert(filtered_range_class != NULL);
  jmethodID constructor = env->GetMethodID(filtered_range_class, "<init>", "(Lnet/quasardb/qdb/QdbTimeRange;Lnet/quasardb/qdb/jni/qdb_ts_filter;)V");
  assert(constructor != NULL);

  jobject range, filter;

  nativeToRange(env, native.range, &range);
  nativeToFilter(env, native.filter, &filter);

  *output = env->NewObject(filtered_range_class,
                           constructor,
                           range,
                           filter);
}

void
columnsToNative(JNIEnv * env, jobjectArray columns, qdb_ts_column_info_t * native_columns, size_t column_count) {
  jfieldID nameField, typeField;
  jclass objectClass;
  for (size_t i = 0; i < column_count; ++i) {
      jobject object =
          (jobject)(env->GetObjectArrayElement(columns, static_cast<jsize>(i)));

      objectClass = env->GetObjectClass(object);
      nameField = env->GetFieldID(objectClass, "name", "Ljava/lang/String;");
      typeField = env->GetFieldID(objectClass, "type", "I");
      jstring name = (jstring)env->GetObjectField(object, nameField);

      native_columns[i].type = static_cast<qdb_ts_column_type_t>(
          env->GetIntField(object, typeField));

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4996) // 'strdup': The POSIX name for this item is deprecated. Instead, use the ISO C and C++ conformant name: _strdup.
#endif
      // Is there a better way to do this? Because we're using strdup here, we
      // need a separate release function which is fragile.
      native_columns[i].name = strdup(StringUTFChars(env, name));
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

void
nativeToColumns(JNIEnv * env, qdb_ts_column_info_t * nativeColumns, size_t column_count, jobjectArray * columns) {
  jclass column_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_column_info");
  assert(column_class != NULL);
  jmethodID constructor = env->GetMethodID(column_class, "<init>", "(Ljava/lang/String;I)V");
  assert(constructor != NULL);

  *columns = env->NewObjectArray((jsize)column_count, column_class, NULL);

  for (size_t i = 0; i < column_count; i++) {
    env->SetObjectArrayElement(*columns, (jsize)i, env->NewObject(column_class,
                                                                  constructor,
                                                                  env->NewStringUTF(nativeColumns[i].name),
                                                                  nativeColumns[i].type));
  }
}

void
doublePointToNative(JNIEnv * env, jobject input, qdb_ts_double_point * native) {
  jfieldID timestampField, valueField;
  jclass objectClass;

  objectClass = env->GetObjectClass(input);

  timestampField = env->GetFieldID(objectClass, "timestamp", "Lnet/quasardb/qdb/QdbTimespec;");
  valueField = env->GetFieldID(objectClass, "value", "D");

  timespecToNative(env, env->GetObjectField(input, timestampField), &(native->timestamp));
  native->value = env->GetDoubleField(input, valueField);
}

void
doublePointsToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_double_point * native) {
  qdb_ts_double_point * cur = native;
  for (size_t i = 0; i < count; ++i) {
      jobject point =
          (jobject)(env->GetObjectArrayElement(input, static_cast<jsize>(i)));

      doublePointToNative(env, point, cur++);
  }
}

void
nativeToDoublePoint(JNIEnv * env, qdb_ts_double_point native, jobject * output) {
  jclass pointClass = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_point");
  assert(pointClass != NULL);
  jmethodID constructor = env->GetMethodID(pointClass, "<init>", "(Lnet/quasardb/qdb/QdbTimespec;D)V");
  assert(constructor != NULL);

  jobject timespec;
  nativeToTimespec(env, native.timestamp, &timespec);

  *output = env->NewObject(pointClass,
                           constructor,
                           timespec,
                           native.value);
}

void
nativeToDoublePoints(JNIEnv * env, qdb_ts_double_point * native, size_t count, jobjectArray * output) {
  jclass pointClass = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_point");
  assert(pointClass != NULL);

  *output = env->NewObjectArray((jsize)count, pointClass, NULL);

  for (size_t i = 0; i < count; i++) {
    jobject point;
    nativeToDoublePoint(env, native[i], &point);
    env->SetObjectArrayElement(*output, (jsize)i, point);
  }
}

void
blobPointToNative(JNIEnv * env, jobject input, qdb_ts_blob_point * native) {
  jfieldID timestampField, valueField;
  jclass objectClass;
  jobject value;

  objectClass = env->GetObjectClass(input);

  timestampField = env->GetFieldID(objectClass, "timestamp", "Lnet/quasardb/qdb/QdbTimespec;");
  valueField = env->GetFieldID(objectClass, "value", "Ljava/nio/ByteBuffer;");
  value = env->GetObjectField(input, valueField);

  timespecToNative(env, env->GetObjectField(input, timestampField), &(native->timestamp));
  native->content = env->GetDirectBufferAddress(value);
  native->content_length = (qdb_size_t)env->GetDirectBufferCapacity(value);
}

void
blobPointsToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_blob_point * native) {
  qdb_ts_blob_point * cur = native;
  for (size_t i = 0; i < count; ++i) {
      jobject point =
          (jobject)(env->GetObjectArrayElement(input, static_cast<jsize>(i)));

      blobPointToNative(env, point, cur++);
  }
}

void
nativeToByteBuffer(JNIEnv * env, void const * content, qdb_size_t contentLength, jobject * output) {
  *output = env->NewDirectByteBuffer((void *)(content), contentLength);
}

void
nativeToBlobPoint(JNIEnv * env, qdb_ts_blob_point native, jobject * output) {
  jclass pointClass = env->FindClass("net/quasardb/qdb/jni/qdb_ts_blob_point");
  assert(pointClass != NULL);
  jmethodID constructor = env->GetMethodID(pointClass, "<init>", "(Lnet/quasardb/qdb/QdbTimespec;Ljava/nio/ByteBuffer;)V");
  assert(constructor != NULL);

  jobject timespec, value;
  nativeToTimespec(env, native.timestamp, &timespec);
  nativeToByteBuffer(env, native.content, native.content_length, &value);

  *output = env->NewObject(pointClass,
                           constructor,
                           timespec,
                           value);
}

void
nativeToBlobPoints(JNIEnv * env, qdb_ts_blob_point * native, size_t count, jobjectArray * output) {
  jclass pointClass = env->FindClass("net/quasardb/qdb/jni/qdb_ts_blob_point");
  assert(pointClass != NULL);

  *output = env->NewObjectArray((jsize)count, pointClass, NULL);

  for (size_t i = 0; i < count; i++) {
    jobject point;
    nativeToBlobPoint(env, native[i], &point);
    env->SetObjectArrayElement(*output, (jsize)i, point);
  }
}

void
doubleAggregateToNative(JNIEnv *env, jobject input, qdb_ts_double_aggregation_t * native) {
  assert(input != NULL);

  jfieldID typeField, filteredRangeField, countField, resultField;
  jclass objectClass;

  objectClass = env->GetObjectClass(input);
  typeField = env->GetFieldID(objectClass, "aggregation_type", "J");
  filteredRangeField = env->GetFieldID(objectClass, "filtered_range", "Lnet/quasardb/qdb/QdbFilteredRange;");
  countField = env->GetFieldID(objectClass, "count", "J");
  resultField = env->GetFieldID(objectClass, "result", "Lnet/quasardb/qdb/jni/qdb_ts_double_point;");

  filteredRangeToNative(env, env->GetObjectField(input, filteredRangeField), &(native->filtered_range));
  doublePointToNative(env, env->GetObjectField(input, resultField), &(native->result));

  native->type = static_cast<qdb_ts_aggregation_type_t>(
      env->GetLongField(input, typeField));
  native->count = static_cast<qdb_size_t>(env->GetLongField(input, countField));
}

void
doubleAggregatesToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_double_aggregation_t * native) {
  assert(input != NULL);

  qdb_ts_double_aggregation_t * cur = native;
  for (size_t i = 0; i < count; ++i) {
      jobject aggregate =
          (jobject)(env->GetObjectArrayElement(input, static_cast<jsize>(i)));

      doubleAggregateToNative(env, aggregate, cur++);
  }
}

void
nativeToDoubleAggregate(JNIEnv * env, qdb_ts_double_aggregation_t native, jobject * output) {
  jclass point_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_aggregation");
  assert(point_class != NULL);
  jmethodID constructor = env->GetMethodID(point_class, "<init>", "(Lnet/quasardb/qdb/QdbFilteredRange;JJLnet/quasardb/qdb/jni/qdb_ts_double_point;)V");
  assert(constructor != NULL);

  jobject filteredRange, result;

  nativeToFilteredRange(env, native.filtered_range, &filteredRange);
  nativeToDoublePoint(env, native.result, &result);

  jobject aggregate = env->NewObject(point_class,
                                     constructor,
                                     filteredRange,
                                     (jlong)native.type,
                                     (jlong)native.count,
                                     result);

  *output = aggregate;
}

void
nativeToDoubleAggregates(JNIEnv * env, qdb_ts_double_aggregation_t * native, size_t count, jobjectArray * output) {
  jclass aggregate_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_aggregation");
  assert(aggregate_class != NULL);

  *output = env->NewObjectArray((jsize)count, aggregate_class, NULL);

  for (size_t i = 0; i < count; i++) {
    jobject aggregate;

    nativeToDoubleAggregate(env, native[i], &aggregate);
    env->SetObjectArrayElement(*output, (jsize)i, aggregate);
  }
}

void
blobAggregateToNative(JNIEnv *env, jobject input, qdb_ts_blob_aggregation_t * native) {
  assert(input != NULL);

  jfieldID typeField, filteredRangeField, countField, resultField;
  jclass objectClass;

  objectClass = env->GetObjectClass(input);
  typeField = env->GetFieldID(objectClass, "aggregation_type", "J");
  filteredRangeField = env->GetFieldID(objectClass, "filtered_range", "Lnet/quasardb/qdb/QdbFilteredRange;");
  countField = env->GetFieldID(objectClass, "count", "J");
  resultField = env->GetFieldID(objectClass, "result", "Lnet/quasardb/qdb/jni/qdb_ts_blob_point;");

  filteredRangeToNative(env, env->GetObjectField(input, filteredRangeField), &(native->filtered_range));
  blobPointToNative(env, env->GetObjectField(input, resultField), &(native->result));

  native->type = static_cast<qdb_ts_aggregation_type_t>(
      env->GetLongField(input, typeField));
  native->count = static_cast<qdb_size_t>(env->GetLongField(input, countField));
}

void
blobAggregatesToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_blob_aggregation_t * native) {
  assert(input != NULL);

  qdb_ts_blob_aggregation_t * cur = native;
  for (size_t i = 0; i < count; ++i) {
    jobject aggregate =
          (jobject)(env->GetObjectArrayElement(input, static_cast<jsize>(i)));

      blobAggregateToNative(env, aggregate, cur++);
  }
}

void
nativeToBlobAggregate(JNIEnv * env, qdb_ts_blob_aggregation_t native, jobject * output) {
  jclass point_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_blob_aggregation");
  assert(point_class != NULL);
  jmethodID constructor = env->GetMethodID(point_class, "<init>", "(Lnet/quasardb/qdb/QdbFilteredRange;JJLnet/quasardb/qdb/jni/qdb_ts_blob_point;)V");
  assert(constructor != NULL);

  jobject filteredRange, result;

  nativeToFilteredRange(env, native.filtered_range, &filteredRange);
  nativeToBlobPoint(env, native.result, &result);

  jobject aggregate = env->NewObject(point_class,
                                     constructor,
                                     filteredRange,
                                     (jlong)native.type,
                                     (jlong)native.count,
                                     result);

  *output = aggregate;
}

void
nativeToBlobAggregates(JNIEnv * env, qdb_ts_blob_aggregation_t * native, size_t count, jobjectArray * output) {
  jclass aggregate_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_blob_aggregation");
  assert(aggregate_class != NULL);

  *output = env->NewObjectArray((jsize)count, aggregate_class, NULL);

  for (size_t i = 0; i < count; i++) {
    jobject aggregate;

    nativeToBlobAggregate(env, native[i], &aggregate);
    env->SetObjectArrayElement(*output, (jsize)i, aggregate);
  }
}

void
printObjectClass(JNIEnv * env, jobject value) {
  jclass cls = env->GetObjectClass(value);

  // First get the class object
  jmethodID mid = env->GetMethodID(cls, "getClass", "()Ljava/lang/Class;");
  jobject clsObj = env->CallObjectMethod(value, mid);

  // Now get the class object's class descriptor
  cls = env->GetObjectClass(clsObj);

  // Find the getName() method on the class object
  mid = env->GetMethodID(cls, "getName", "()Ljava/lang/String;");

  // Call the getName() to get a jstring object back
  jstring strObj = (jstring)env->CallObjectMethod(clsObj, mid);

  // Now get the c string from the java jstring object
  const char* str = env->GetStringUTFChars(strObj, NULL);

  // Print the class name
  printf("\nCalling class is: %s\n", str);

  // Release the memory pinned char array
  env->ReleaseStringUTFChars(strObj, str);
}

qdb_ts_column_type_t
columnTypeFromColumnValue(JNIEnv * env, jobject value) {
  jclass objectClass;
  jobject typeObject;
  // jfieldID typeField;
  jfieldID typeValueField;
  jmethodID methodId;

  objectClass = env->GetObjectClass(value);

  // First get the
  methodId = env->GetMethodID(objectClass, "getType", "()Lnet/quasardb/qdb/QdbTimeSeriesValue$Type;");
  typeObject = env->CallObjectMethod(value, methodId);

  objectClass = env->GetObjectClass(typeObject);

  typeValueField = env->GetFieldID(objectClass, "value", "I");
  return (qdb_ts_column_type_t)(env->GetIntField(typeObject, typeValueField));
}

qdb_error_t
tableRowSetInt64ColumnValue(JNIEnv * env, qdb_local_table_t localTable, size_t columnIndex, jobject value) {
  jclass objectClass = env->GetObjectClass(value);
  jmethodID methodId = env->GetMethodID(objectClass, "getInt64", "()J");
  assert(methodId != NULL);

  return qdb_ts_row_set_int64(localTable, columnIndex, env->CallLongMethod(value, methodId));
}

qdb_error_t
tableRowSetDoubleColumnValue(JNIEnv * env, qdb_local_table_t localTable, size_t columnIndex, jobject value) {
  jclass objectClass = env->GetObjectClass(value);
  jmethodID methodId = env->GetMethodID(objectClass, "getDouble", "()D");
  assert(methodId != NULL);

  return qdb_ts_row_set_double(localTable, columnIndex, env->CallDoubleMethod(value, methodId));
}

qdb_error_t
tableRowSetTimestampColumnValue(JNIEnv * env, qdb_local_table_t localTable, size_t columnIndex, jobject value) {
  jclass objectClass = env->GetObjectClass(value);
  jmethodID methodId = env->GetMethodID(objectClass, "getTimestamp", "()Lnet/quasardb/qdb/QdbTimespec;");
  assert(methodId != NULL);

  jobject timestampObject = env->CallObjectMethod(value, methodId);
  assert(timestampObject != NULL);

  qdb_timespec_t timestamp;
  timespecToNative(env, timestampObject, &timestamp);

  return qdb_ts_row_set_timestamp(localTable, columnIndex, &timestamp);
}

qdb_error_t
tableRowSetBlobColumnValue(JNIEnv * env, qdb_local_table_t localTable, size_t columnIndex, jobject value) {
  jclass objectClass = env->GetObjectClass(value);
  jmethodID methodId = env->GetMethodID(objectClass, "getBlob", "()Ljava/nio/ByteBuffer;");
  assert(methodId != NULL);

  jobject blobValue = env->CallObjectMethod(value, methodId);

  return qdb_ts_row_set_blob(localTable,
                             columnIndex,
                             env->GetDirectBufferAddress(blobValue),
                             (qdb_size_t)env->GetDirectBufferCapacity(blobValue));
}



qdb_error_t
tableRowSetColumnValue(JNIEnv * env, qdb_local_table_t localTable, size_t columnIndex, jobject value) {
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
tableRowAppend(JNIEnv * env, qdb_local_table_t localTable, jobject time, jobjectArray values, size_t count, qdb_size_t * rowIndex) {
  qdb_timespec_t nativeTime;
  timespecToNative(env, time, &nativeTime);

  for (size_t i = 0; i < count; i++) {
    jobject value =
      (jobject)(env->GetObjectArrayElement(values, static_cast<jsize>(i)));
    qdb_error_t err = tableRowSetColumnValue(env, localTable, i, value);

    if (!QDB_SUCCESS(err)) {
      return err;
    }
  }

  return qdb_ts_table_row_append(localTable, &nativeTime, rowIndex);
}


qdb_error_t
tableGetRanges(JNIEnv *env, qdb_local_table_t localTable, jobjectArray ranges) {
  qdb_size_t rangesCount = env->GetArrayLength(ranges);
  qdb_ts_filtered_range_t * nativeRanges =
    (qdb_ts_filtered_range_t *)(malloc(rangesCount * sizeof(qdb_ts_filtered_range_t)));

  filteredRangesToNative(env, ranges, rangesCount, nativeRanges);

  qdb_error_t err = qdb_ts_table_get_ranges(localTable, nativeRanges, rangesCount);

  free(nativeRanges);
  return err;
}

qdb_error_t
tableGetRowInt64Value(JNIEnv *env, qdb_local_table_t localTable, qdb_size_t index, jobject output) {
  jlong value;
  qdb_error_t err = qdb_ts_row_get_int64(localTable, index, &value);

  if (QDB_SUCCESS(err)) {
    jclass objectClass = env->GetObjectClass(output);
    jmethodID methodId = env->GetMethodID(objectClass, "setInt64", "(J)V");
    assert(methodId != NULL);

    env->CallVoidMethod(output, methodId, value);
  }

  return err;
}

void
tableGetRowNullValue(JNIEnv *env, qdb_local_table_t localTable, qdb_size_t index, jobject output) {
  jclass objectClass = env->GetObjectClass(output);
  jmethodID methodId = env->GetMethodID(objectClass, "setNull", "()V");
  assert(methodId != NULL);

  env->CallVoidMethod(output, methodId);
}

qdb_error_t
tableGetRowDoubleValue(JNIEnv *env, qdb_local_table_t localTable, qdb_size_t index, jobject output) {
  double value;
  qdb_error_t err = qdb_ts_row_get_double(localTable, index, &value);

  if (QDB_SUCCESS(err)) {
    jclass objectClass = env->GetObjectClass(output);
    jmethodID methodId = env->GetMethodID(objectClass, "setDouble", "(D)V");
    assert(methodId != NULL);

    env->CallVoidMethod(output, methodId, value);
  }

  return err;
}

qdb_error_t
tableGetRowTimestampValue(JNIEnv *env, qdb_local_table_t localTable, qdb_size_t index, jobject output) {

  qdb_timespec_t value;

  qdb_error_t err = qdb_ts_row_get_timestamp(localTable, index, &value);

  if (QDB_SUCCESS(err)) {
    jobject timestampObject;
    nativeToTimespec(env, value, &timestampObject);
    assert(timestampObject != NULL);

    jclass objectClass = env->GetObjectClass(output);
    jmethodID methodId = env->GetMethodID(objectClass, "setTimestamp", "(Lnet/quasardb/qdb/QdbTimespec;)V");
    assert(methodId != NULL);

    env->CallVoidMethod(output, methodId, timestampObject);
  }

  return err;
}

qdb_error_t
tableGetRowBlobValue(JNIEnv *env, qdb_local_table_t localTable, qdb_size_t index, jobject output) {

  void const * value;
  qdb_size_t length;

  qdb_error_t err = qdb_ts_row_get_blob(localTable, index, &value, &length);

  if (QDB_SUCCESS(err)) {
    assert(value != NULL);

    jobject byteBuffer;
    nativeToByteBuffer(env, value, length, &byteBuffer);
    assert(byteBuffer != NULL);

    jclass objectClass = env->GetObjectClass(output);
    jmethodID methodId = env->GetMethodID(objectClass, "setBlob", "(Ljava/nio/ByteBuffer;)V");
    assert(methodId != NULL);

    env->CallVoidMethod(output, methodId, byteBuffer);
  }

  return err;
}

qdb_error_t
tableGetRowValues (JNIEnv *env, qdb_local_table_t localTable, qdb_ts_column_info_t * columns, qdb_size_t count, jobjectArray values) {
  qdb_error_t err;

  jclass valueClass = env->FindClass("net/quasardb/qdb/QdbTimeSeriesValue");
  assert(valueClass != NULL);
  jmethodID constructor = env->GetStaticMethodID(valueClass, "createNull", "()Lnet/quasardb/qdb/QdbTimeSeriesValue;");
  assert(constructor != NULL);


  for (size_t i = 0; i < count; ++i) {
    qdb_ts_column_info_t column = columns[i];

    jobject value = env->CallStaticObjectMethod(valueClass, constructor);
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

    env->SetObjectArrayElement(values, (jsize)i, value);
  }

  return qdb_e_ok;
}

qdb_error_t
tableGetRow(JNIEnv *env, qdb_local_table_t localTable, qdb_ts_column_info_t * columns, qdb_size_t columnCount, jobject * output) {

  qdb_timespec_t timestamp;
  qdb_error_t err = qdb_ts_table_next_row(localTable, &timestamp);

  if (err == qdb_e_iterator_end) {
    return err;
  }

  if (QDB_SUCCESS(err)) {
    jclass value_class = env->FindClass("net/quasardb/qdb/QdbTimeSeriesValue");
    assert(value_class != NULL);
    jobjectArray values = env->NewObjectArray((jsize)columnCount, value_class, NULL);

    err = tableGetRowValues(env, localTable, columns, columnCount, values);

    if (QDB_SUCCESS(err)) {
      jclass row_class = env->FindClass("net/quasardb/qdb/QdbTimeSeriesRow");
      assert(row_class != NULL);

      jmethodID constructor = env->GetMethodID(row_class, "<init>", "(Lnet/quasardb/qdb/QdbTimespec;[Lnet/quasardb/qdb/QdbTimeSeriesValue;)V");
      assert(constructor != NULL);

      jobject timespec;
      nativeToTimespec(env, timestamp, &timespec);
      assert(timespec != NULL);

      *output = env->NewObject(row_class,
                               constructor,
                               timespec,
                               values);
    }
  }

  return err;
}
