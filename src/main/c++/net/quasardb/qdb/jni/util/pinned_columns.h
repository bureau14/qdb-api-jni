#pragma once

#include <qdb/client.h>
#include <qdb/ts.h>


#include "../exception.h"
#include "../object_array.h"
#include "../byte_buffer.h"
#include "../primitive_array.h"

namespace qdb
{
namespace jni
{

class env;

template <typename T, typename U = T>
struct column_pinner
{
  void pin(qdb::jni::env & env,
           qdb_handle_t handle,
           qdb_batch_table_t table,
           qdb_size_t index,
           qdb_size_t capacity,
           qdb_timespec_t * timestamp,
           qdb_time_t ** timeoffsets,
           T ** data);

  void copy(qdb::jni::env & env,
            jlong const * in_timeoffsets, T const * in_data,
            qdb_time_t * out_timeoffsets, T * out_data,
            qdb_size_t len);


  void copy(qdb::jni::env & env,
            jlong const * in_timeoffsets, T const & in_data,
            qdb_time_t * out_timeoffsets, T * out_data,
            qdb_size_t len);

  void copy2(qdb::jni::env & env,
             jlong const * in_timeoffsets, T * in1_data, T * in2_data,
             qdb_time_t * out_timeoffsets, U * out_data,
             qdb_size_t len);

};

template <>
struct column_pinner<double>
{
  void pin(qdb::jni::env & env,
           qdb_handle_t handle,
           qdb_batch_table_t table,
           qdb_size_t index,
           qdb_size_t capacity,
           qdb_timespec_t * timestamp,
           qdb_time_t ** timeoffsets,
           double ** data);

  void copy(qdb::jni::env & env,
            jlong const * in_timeoffsets,
            double const * in_data,
            qdb_time_t * out_timeoffsets,
            double * out_data,
            qdb_size_t len);
};

template <>
struct column_pinner<qdb_int_t>
{
  void pin(qdb::jni::env & env,
           qdb_handle_t handle,
           qdb_batch_table_t table,
           qdb_size_t index,
           qdb_size_t capacity,
           qdb_timespec_t * timestamp,
           qdb_time_t ** timeoffsets,
           qdb_int_t ** data);

  void copy(qdb::jni::env & env,
            jlong const * in_timeoffsets,
            qdb_int_t const * in_data,
            qdb_time_t * out_timeoffsets,
            qdb_int_t * out_data,
            qdb_size_t len);
};

template <>
struct column_pinner<jni::object_array, qdb_blob_t>
{
  void pin(qdb::jni::env & env,
           qdb_handle_t handle,
           qdb_batch_table_t table,
           qdb_size_t index,
           qdb_size_t capacity,
           qdb_timespec_t * timestamp,
           qdb_time_t ** timeoffsets,
           qdb_blob_t ** data);

  void copy(qdb::jni::env & env,
            jlong const * in_timeoffsets,
            jni::object_array const & in_data,
            qdb_time_t * out_timeoffsets,
            qdb_blob_t * out_data,
            qdb_size_t len);
};



template <>
struct column_pinner<jni::object_array, qdb_string_t>
{
  void pin(qdb::jni::env & env,
           qdb_handle_t handle,
           qdb_batch_table_t table,
           qdb_size_t index,
           qdb_size_t capacity,
           qdb_timespec_t * timestamp,
           qdb_time_t ** timeoffsets,
           qdb_string_t ** data);

  void copy(qdb::jni::env & env,
            jlong const * in_timeoffsets,
            jni::object_array const & in_data,
            qdb_time_t * out_timeoffsets,
            qdb_string_t * out_data,
            qdb_size_t len);
};


template <>
struct column_pinner<jlong, qdb_timespec_t>
{
  void pin(qdb::jni::env & env,
           qdb_handle_t handle,
           qdb_batch_table_t table,
           qdb_size_t index,
           qdb_size_t capacity,
           qdb_timespec_t * timestamp,
           qdb_time_t ** timeoffsets,
           qdb_timespec_t ** data);

  void copy2(qdb::jni::env & env,
             jlong const * in_timeoffsets,
             jlong const * in1_data,
             jlong const * in2_data,
             qdb_time_t * out_timeoffsets,
             qdb_timespec_t * out_data,
             qdb_size_t len);
};

template <typename T, typename U=T> static inline jint
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
  U * data                 = NULL;

  column_pinner<T, U> pinner {};

  // qdb_ts_column_type_t * column_types[column_count];
  // to_column_type_array(env, columns, column_types, column_count);
  auto values_guard       = jni::primitive_array::get_array_critical<T>(env, values);
  auto timeoffsets_guard  = jni::primitive_array::get_array_critical<jlong>(env, timeoffsets_);

  assert(values_guard.size () == timeoffsets_guard.size());

  /**
   * Push a single shard using pinned columns. We first pin all the columns
   * that we expect.
   */
  pinner.pin(env,
             handle,
             table,
             columnIndex,
             values_guard.size(),
             &shard,
             &timeoffsets,
             &data);

  assert(timeoffsets != NULL);
  assert(data != NULL);


  pinner.copy(env,
              timeoffsets_guard.get(), values_guard.get(),
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
  auto values1_guard      = jni::primitive_array::get_array_critical<T>(env, values1);
  auto values2_guard      = jni::primitive_array::get_array_critical<T>(env, values2);
  auto timeoffsets_guard  = jni::primitive_array::get_array_critical<jlong>(env, timeoffsets_);

  assert(values1_guard.size () == timeoffsets_guard.size());
  assert(values2_guard.size () == timeoffsets_guard.size());

  /**
   * Push a single shard using pinned columns. We first pin all the columns
   * that we expect.
   */
  pinner.pin(env,
             handle,
             table,
             columnIndex,
             timeoffsets_guard.size(),
             &shard,
             &timeoffsets,
             &data);

  assert(timeoffsets != NULL);
  assert(data != NULL);


  pinner.copy2(env,
               timeoffsets_guard.get(), values1_guard.get(), values2_guard.get(),
               timeoffsets, data,
               timeoffsets_guard.size());

  return qdb_e_ok;

}


template <typename T, typename U=T> static inline jint
set_pinned_objects(qdb::jni::env & env,
                   jlong handle_,
                   jlong table_,
                   jlong shard_,
                   jint columnIndex,
                   jlongArray timeoffsets_,
                   jobjectArray values_)
{
  qdb::jni::log::swap_callback();

  qdb_handle_t handle      = reinterpret_cast<qdb_handle_t>(handle_);
  qdb_batch_table_t table  = reinterpret_cast<qdb_batch_table_t>(table_);
  qdb_timespec_t shard     = qdb_timespec_t {shard_, 0};
  qdb_time_t * timeoffsets = NULL;
  U * data                 = NULL;

  column_pinner<T, U> pinner {};

  // qdb_ts_column_type_t * column_types[column_count];
  // to_column_type_array(env, columns, column_types, column_count);

  object_array values(env, values_);
  auto timeoffsets_guard  = jni::primitive_array::get_array_critical<jlong>(env, timeoffsets_);

  assert(values.size () == timeoffsets_guard.size());

  /**
   * Push a single shard using pinned columns. We first pin all the columns
   * that we expect.
   */
  pinner.pin(env,
             handle,
             table,
             columnIndex,
             timeoffsets_guard.size(),
             &shard,
             &timeoffsets,
             &data);

  assert(timeoffsets != NULL);
  assert(data != NULL);

  pinner.copy(env,
              timeoffsets_guard.get(), values,
              timeoffsets, data,
              timeoffsets_guard.size());

  return qdb_e_ok;
}


};
};
