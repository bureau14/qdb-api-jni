#pragma once

#include <jni.h>

#include <qdb/client.h>
#include <qdb/ts.h>

namespace qdb
{
namespace jni
{

class env;
class object_array;

/**
 * T = input type, U = output type, e.g. T = jlong, U = qdb_int_t
 */
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
           U ** data);

  void copy(qdb::jni::env & env,
            jlong const * in_timeoffsets, T const * in_data,
            qdb_time_t * out_timeoffsets, T * out_data,
            qdb_size_t len);

  /**
   * Copy data from two columns rather than just one.
   */
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
struct column_pinner<jlong, qdb_int_t>
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
            jlong const * in_data,
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

};
};
