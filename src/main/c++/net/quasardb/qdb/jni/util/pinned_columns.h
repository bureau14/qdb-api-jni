#pragma once

#include "../primitive_array.h"

namespace qdb
{
namespace jni
{

class env;

template <typename T, typename U = T>
struct column_pinner
{
  void pin(qdb_handle_t handle,
           qdb_batch_table_t table,
           qdb_size_t index,
           qdb_size_t capacity,
           qdb_timespec_t * timestamp,
           qdb_time_t ** timeoffsets,
           T ** data);

  void copy(long * in_timeoffsets, T * in_data,
            long * out_timeoffsets, T * out_data,
            qdb_size_t len);

  void copy2(long * in_timeoffsets, T * in1_data, T * in2_data,
             long * out_timeoffsets, U * out_data,
             qdb_size_t len);

};

template <>
struct column_pinner<double>
{
  void pin(qdb_handle_t handle,
           qdb_batch_table_t table,
           qdb_size_t index,
           qdb_size_t capacity,
           qdb_timespec_t * timestamp,
           qdb_time_t ** timeoffsets,
           double ** data) {
  jni::exception::throw_if_error(handle,
                                 qdb_ts_batch_pin_double_column(table,
                                                                index,
                                                                capacity,
                                                                timestamp,
                                                                timeoffsets,
                                                                data));

  }

  void copy(long * in_timeoffsets,
            double * in_data,
            long * out_timeoffsets,
            double * out_data,
            qdb_size_t len) {

    for (qdb_size_t i = 0; i < len; ++i) {
      if (isnan(in_data[i])) {
        // Skip null values entirely
        continue;
      }

      out_timeoffsets[i] = in_timeoffsets[i];
      out_data[i] = in_data[i];
    }
  }
};

template <>
struct column_pinner<qdb_int_t>
{
  void pin(qdb_handle_t handle,
           qdb_batch_table_t table,
           qdb_size_t index,
           qdb_size_t capacity,
           qdb_timespec_t * timestamp,
           qdb_time_t ** timeoffsets,
           qdb_int_t ** data) {
    jni::exception::throw_if_error(handle,
                                   qdb_ts_batch_pin_int64_column(table,
                                                                 index,
                                                                 capacity,
                                                                 timestamp,
                                                                 timeoffsets,
                                                                 data));

  }

  void copy(long * in_timeoffsets,
            qdb_int_t * in_data,
            long * out_timeoffsets,
            qdb_int_t * out_data,
            qdb_size_t len) {

    for (qdb_size_t i = 0; i < len; ++i) {
      if (in_data[i] == (qdb_int_t)0x8000000000000000ll) {
        // Skip null values entirely
        continue;
      }

      out_timeoffsets[i] = in_timeoffsets[i];
      out_data[i] = in_data[i];
    }
  }
};



template <>
struct column_pinner<long, qdb_timespec_t>
{
  void pin(qdb_handle_t handle,
           qdb_batch_table_t table,
           qdb_size_t index,
           qdb_size_t capacity,
           qdb_timespec_t * timestamp,
           qdb_time_t ** timeoffsets,
           qdb_timespec_t ** data) {
  jni::exception::throw_if_error(handle,
                                 qdb_ts_batch_pin_timestamp_column(table,
                                                                   index,
                                                                   capacity,
                                                                   timestamp,
                                                                   timeoffsets,
                                                                   data));

  }

  void copy2(long * in_timeoffsets,
             long * in1_data,
             long * in2_data,
             long * out_timeoffsets,
             qdb_timespec_t * out_data,
             qdb_size_t len) {

    for (qdb_size_t i = 0; i < len; ++i) {
      if (in1_data[i] == qdb_min_time &&
          in2_data[i] == qdb_min_time) {
        // Skip null values entirely
        continue;
      }

      out_timeoffsets[i]  = in_timeoffsets[i];
      out_data[i].tv_sec  = in1_data[i];
      out_data[i].tv_nsec = in2_data[i];
    }
  }
};




template <typename T> static inline jint
set_pinned(qdb::jni::env & env,
           jlong handle_,
           jlong table_,
           jlong shard_,
           jint columnIndex,
           jlongArray timeoffsets_,
           jarray values)
{
  qdb::jni::log::swap_callback();

  qdb_handle_t handle      = reinterpret_cast<qdb_handle_t>(handle_);
  qdb_batch_table_t table  = reinterpret_cast<qdb_batch_table_t>(table_);
  qdb_timespec_t shard     = qdb_timespec_t {shard_, 0};
  qdb_time_t * timeoffsets = NULL;
  T * data                 = NULL;


  column_pinner<T> pinner {};

  // qdb_ts_column_type_t * column_types[column_count];
  // to_column_type_array(env, columns, column_types, column_count);
  auto values_guard       = jni::primitive_array::get_array_critical<T>(env, values);
  auto timeoffsets_guard  = jni::primitive_array::get_array_critical<long>(env, timeoffsets_);

  assert(values_guard.size () == timeoffsets_guard.size());

  /**
   * Push a single shard using pinned columns. We first pin all the columns
   * that we expect.
   */
  pinner.pin(handle,
             table,
             columnIndex,
             values_guard.size(),
             &shard,
             &timeoffsets,
             &data);

  assert(timeoffsets != NULL);
  assert(data != NULL);


  pinner.copy(timeoffsets_guard.get(), values_guard.get(),
              timeoffsets, data,
              values_guard.size());

  return qdb_e_ok;
}

template <typename T, typename U> static inline jint
set_pinned2(qdb::jni::env & env,
            jlong handle_,
            jlong table_,
            jlong shard_,
            jint columnIndex,
            jlongArray timeoffsets_,
            jarray values1,
            jarray values2) {
  qdb::jni::log::swap_callback();

  qdb_handle_t handle      = reinterpret_cast<qdb_handle_t>(handle_);
  qdb_batch_table_t table  = reinterpret_cast<qdb_batch_table_t>(table_);
  qdb_timespec_t shard     = qdb_timespec_t {shard_, 0};
  qdb_time_t * timeoffsets = NULL;
  U * data                 = NULL;


  column_pinner<T, U> pinner {};

  // qdb_ts_column_type_t * column_types[column_count];
  // to_column_type_array(env, columns, column_types, column_count);
  auto values1_guard       = jni::primitive_array::get_array_critical<T>(env, values1);
  auto values2_guard       = jni::primitive_array::get_array_critical<T>(env, values2);
  auto timeoffsets_guard  = jni::primitive_array::get_array_critical<long>(env, timeoffsets_);

  assert(values1_guard.size () == timeoffsets_guard.size());
  assert(values2_guard.size () == timeoffsets_guard.size());

  /**
   * Push a single shard using pinned columns. We first pin all the columns
   * that we expect.
   */
  pinner.pin(handle,
             table,
             columnIndex,
             timeoffsets_guard.size(),
             &shard,
             &timeoffsets,
             &data);

  assert(timeoffsets != NULL);
  assert(data != NULL);


  pinner.copy2(timeoffsets_guard.get(), values1_guard.get(), values2_guard.get(),
               timeoffsets, data,
               timeoffsets_guard.size());

  return qdb_e_ok;

}


};
};
