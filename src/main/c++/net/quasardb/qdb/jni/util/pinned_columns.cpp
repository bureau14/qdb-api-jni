#include "pinned_columns.h"

#include "../byte_buffer.h"
#include "../exception.h"

namespace qdb
{
namespace jni
{


/**
 * Double
 */

void
column_pinner<double>::pin(qdb::jni::env & env,
                           qdb_handle_t handle,
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

void
column_pinner<double>::copy(qdb::jni::env & env,
                            jlong const * in_timeoffsets,
                            double const * in_data,
                            qdb_time_t * out_timeoffsets,
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


/**
 * Int64
 */

void
column_pinner<jlong, qdb_int_t>::pin(qdb::jni::env & env,
                                     qdb_handle_t handle,
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

void
column_pinner<jlong, qdb_int_t>::copy(qdb::jni::env & env,
                                      jlong const * in_timeoffsets,
                                      qdb_int_t const * in_data,
                                      qdb_time_t * out_timeoffsets,
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

/**
 * Blob
 */

void
column_pinner<jni::object_array, qdb_blob_t>::pin(qdb::jni::env & env,
                                                  qdb_handle_t handle,
                                                  qdb_batch_table_t table,
                                                  qdb_size_t index,
                                                  qdb_size_t capacity,
                                                  qdb_timespec_t * timestamp,
                                                  qdb_time_t ** timeoffsets,
                                                  qdb_blob_t ** data) {
  jni::exception::throw_if_error(handle,
                                 qdb_ts_batch_pin_blob_column(table,
                                                              index,
                                                              capacity,
                                                              timestamp,
                                                              timeoffsets,
                                                              data));
}

void
column_pinner<jni::object_array, qdb_blob_t>::copy(qdb::jni::env & env,
                                                   jlong const * in_timeoffsets,
                                                   jni::object_array const & in_data,
                                                   qdb_time_t * out_timeoffsets,
                                                   qdb_blob_t * out_data,
                                                   qdb_size_t len) {
  assert(in_data.size () == len);

  for (qdb_size_t i = 0; i < len; ++i) {
    jobject bb = in_data.get(i);

    if (bb == NULL) {
      continue;
    }

    out_timeoffsets[i] = in_timeoffsets[i];

    jni::byte_buffer::get_address(env,
                                  bb,
                                  &out_data[i].content,
                                  &out_data[i].content_length);
  }
}



/**
 * String
 */

void
column_pinner<jni::object_array, qdb_string_t>::pin(qdb::jni::env & env,
                                                    qdb_handle_t handle,
                                                    qdb_batch_table_t table,
                                                    qdb_size_t index,
                                                    qdb_size_t capacity,
                                                    qdb_timespec_t * timestamp,
                                                    qdb_time_t ** timeoffsets,
                                                    qdb_string_t ** data) {
  jni::exception::throw_if_error(handle,
                                 qdb_ts_batch_pin_string_column(table,
                                                              index,
                                                              capacity,
                                                              timestamp,
                                                              timeoffsets,
                                                              data));
}

void
column_pinner<jni::object_array, qdb_string_t>::copy(qdb::jni::env & env,
                                                   jlong const * in_timeoffsets,
                                                   jni::object_array const & in_data,
                                                   qdb_time_t * out_timeoffsets,
                                                   qdb_string_t * out_data,
                                                   qdb_size_t len) {
  assert(in_data.size () == len);

  for (qdb_size_t i = 0; i < len; ++i) {
    jobject bb = in_data.get(i);

    if (bb == NULL) {
      continue;
    }

    out_timeoffsets[i] = in_timeoffsets[i];

    jni::byte_buffer::get_address(env,
                                  bb,
                                  (void const **)(&out_data[i].data),
                                  &out_data[i].length);
  }
}



/**
 * Timestamp
 */

void
column_pinner<jlong, qdb_timespec_t>::pin(qdb::jni::env & env,
                                          qdb_handle_t handle,
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

void
column_pinner<jlong, qdb_timespec_t>::copy2(qdb::jni::env & env,
                                            jlong const * in_timeoffsets,
                                            jlong const * in1_data,
                                            jlong const * in2_data,
                                            qdb_time_t * out_timeoffsets,
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
};
