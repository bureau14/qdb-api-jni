package net.quasardb.qdb.jni;

import java.nio.ByteBuffer;

public final class qdb {
  static {
    String os = System.getProperty("os.name");
    String arch = System.getProperty("os.arch");

    if (os.startsWith("Windows")) {
      if (arch.equals("x86")) {
        NativeLibraryLoader.load("/net/quasardb/qdb/jni/windows/x86/qdb_api.dll");
        NativeLibraryLoader.load("/net/quasardb/qdb/jni/windows/x86/qdb_api_jni.dll");
      } else {
        NativeLibraryLoader.load("/net/quasardb/qdb/jni/windows/x64/qdb_api.dll");
        NativeLibraryLoader.load("/net/quasardb/qdb/jni/windows/x64/qdb_api_jni.dll");
      }
    } else if (os.startsWith("Mac OS X")) {
      NativeLibraryLoader.load("/net/quasardb/qdb/jni/macosx/x64/libqdb_api.dylib");
      NativeLibraryLoader.load("/net/quasardb/qdb/jni/macosx/x64/libqdb_api_jni.dylib");
    } else if (os.startsWith("Linux")) {
      NativeLibraryLoader.load("/net/quasardb/qdb/jni/linux/x64/libqdb_api.so");
      NativeLibraryLoader.load("/net/quasardb/qdb/jni/linux/x64/libqdb_api_jni.so");
    } else if (os.startsWith("FreeBSD")) {
      NativeLibraryLoader.load("/net/quasardb/qdb/jni/freebsd/x64/libqdb_api.so");
      NativeLibraryLoader.load("/net/quasardb/qdb/jni/freebsd/x64/libqdb_api_jni.so");
    } else {
      throw new RuntimeException("Unsupported operating system: " + os);
    }
  }

  public static native String build();
  public static native String version();
  public static native String error_message(int code);

  public static native long open_tcp();
  public static native int connect(long handle, String uri);
  public static native int close(long handle);
  public static native void release(long handle, ByteBuffer buffer);

  public static native int option_set_timeout(long handle, int millis);
  public static native int purge_all(long handle, int timeout);
  public static native int trim_all(long handle, int timeout);

  public static native int remove(long handle, String alias);
  public static native int expires_at(long handle, String alias, long expiry_time);
  public static native int get_expiry_time(long handle, String alias, Reference<Long> expiry);
  public static native int get_type(long handle, String alias, Reference<Integer> type);
  public static native int get_metadata(long handle, String alias, ByteBuffer metadata);

  public static native int blob_compare_and_swap(long handle, String alias, ByteBuffer newContent, ByteBuffer comparand,
                                                 long expiry, Reference<ByteBuffer> originalContent);
  public static native int blob_put(long handle, String alias, ByteBuffer content, long expiry);
  public static native int blob_get(long handle, String alias, Reference<ByteBuffer> content);
  public static native int blob_get_and_remove(long handle, String alias, Reference<ByteBuffer> content);
  public static native int blob_get_and_update(long handle, String alias, ByteBuffer newContent, long expiry,
                                               Reference<ByteBuffer> originalContent);
  public static native int blob_remove_if(long handle, String alias, ByteBuffer comparand);
  public static native int blob_update(long handle, String alias, ByteBuffer content, long expiry);

  public static native int int_put(long handle, String alias, long value, long expiry);
  public static native int int_update(long handle, String alias, long value, long expiry);
  public static native int int_get(long handle, String alias, Reference<Long> value);
  public static native int int_add(long handle, String alias, long addend, Reference<Long> result);

  public static native int deque_size(long handle, String alias, Reference<Long> size);
  public static native int deque_get_at(long handle, String alias, long index, Reference<ByteBuffer> content);
  public static native int deque_set_at(long handle, String alias, long index, ByteBuffer content);
  public static native int deque_push_front(long handle, String alias, ByteBuffer content);
  public static native int deque_push_back(long handle, String alias, ByteBuffer content);
  public static native int deque_pop_front(long handle, String alias, Reference<ByteBuffer> content);
  public static native int deque_pop_back(long handle, String alias, Reference<ByteBuffer> content);
  public static native int deque_front(long handle, String alias, Reference<ByteBuffer> content);
  public static native int deque_back(long handle, String alias, Reference<ByteBuffer> content);

  public static native int hset_insert(long handle, String alias, ByteBuffer content);
  public static native int hset_erase(long handle, String alias, ByteBuffer content);
  public static native int hset_contains(long handle, String alias, ByteBuffer content);

  public static native int stream_open(long handle, String alias, int mode, Reference<Long> stream);
  public static native int stream_close(long stream);
  public static native int stream_read(long stream, ByteBuffer content, Reference<Long> bytesRead);
  public static native int stream_write(long stream, ByteBuffer content);
  public static native int stream_size(long stream, Reference<Long> size);
  public static native int stream_getpos(long stream, Reference<Long> position);
  public static native int stream_setpos(long stream, long position);
  public static native int stream_truncate(long stream, long position);

  public static native int attach_tag(long handle, String alias, String tag);
  public static native int has_tag(long handle, String alias, String tag);
  public static native int detach_tag(long handle, String alias, String tag);
  public static native int get_tags(long handle, String alias, Reference<String[]> tags);
  public static native int tag_iterator_begin(long handle, String tag, Reference<Long> iterator);
  public static native int tag_iterator_next(long iterator);
  public static native int tag_iterator_close(long iterator);
  public static native String tag_iterator_alias(long iterator);
  public static native int tag_iterator_type(long iterator);

  public static native int ts_create(long handle, String alias, qdb_ts_column_info[] columns);
  public static native int ts_insert_columns(long handle, String alias, qdb_ts_column_info[] columns);
  public static native int ts_list_columns(long handle, String alias, Reference<qdb_ts_column_info[]> columns);
  public static native int ts_double_insert(long handle, String alias, String column, qdb_ts_double_point[] points);
  public static native int ts_double_get_ranges(long handle, String alias, String column, qdb_ts_filtered_range[] ranges,
                                                Reference<qdb_ts_double_point[]> points);
  public static native int ts_double_aggregate(long handle, String alias, String column, qdb_ts_double_aggregation[] input,
                                               Reference<qdb_ts_double_aggregation[]> aggregations);
  public static native int ts_blob_insert(long handle, String alias, String column, qdb_ts_blob_point[] points);
  public static native int ts_blob_get_ranges(long handle, String alias, String column, qdb_ts_filtered_range[] ranges,
                                              Reference<qdb_ts_blob_point[]> points);
  public static native int ts_blob_aggregate(long handle, String alias, String column, qdb_ts_blob_aggregation[] input,
                                             Reference<qdb_ts_blob_aggregation[]> aggregations);

  public static native int node_status(long handle, String uri, Reference<String> content);
  public static native int node_config(long handle, String uri, Reference<String> content);
  public static native int node_topology(long handle, String uri, Reference<String> content);
  public static native int node_stop(long handle, String uri, String reason);
  public static native int get_location(long handle, String alias, Reference<String> address, Reference<Integer> port);

  public static native int init_operations(long handle, int count, Reference<Long> batch);
  public static native int delete_batch(long handle, long batch);
  public static native int run_batch(long handle, long batch, int count);

  public static native void batch_write_blob_compare_and_swap(long batch, int index, String alias,
                                                              ByteBuffer newContent, ByteBuffer comparand, long expiry);
  public static native void batch_write_blob_get(long batch, int index, String alias);
  public static native void batch_write_blob_get_and_update(long batch, int index, String alias, ByteBuffer content,
                                                            long expiry);
  public static native void batch_write_blob_put(long batch, int index, String alias, ByteBuffer content, long expiry);
  public static native void batch_write_blob_update(long batch, int index, String alias, ByteBuffer content,
                                                    long expiry);
  public static native int batch_read_blob_compare_and_swap(long batch, int index, String alias,
                                                            Reference<ByteBuffer> originalContent);
  public static native int batch_read_blob_get(long batch, int index, String alias, Reference<ByteBuffer> content);
  public static native int batch_read_blob_get_and_update(long batch, int index, String alias,
                                                          Reference<ByteBuffer> content);
  public static native int batch_read_blob_put(long batch, int index, String alias);
  public static native int batch_read_blob_update(long batch, int index, String alias);
}
