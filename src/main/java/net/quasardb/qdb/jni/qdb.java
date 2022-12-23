package net.quasardb.qdb.jni;

import java.nio.ByteBuffer;
import net.quasardb.qdb.*;
import net.quasardb.qdb.Logger;
import net.quasardb.qdb.PerformanceTrace;
import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Result;
import net.quasardb.qdb.ts.WritableRow;
import net.quasardb.qdb.ts.Table;
import net.quasardb.qdb.ts.TimeRange;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Timespecs;
import net.quasardb.qdb.ts.Points;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.Writer;

public final class qdb
{
    static
    {
        String os = System.getProperty("os.name");
        String arch = System.getProperty("os.arch");

        if (os.startsWith("Windows"))
        {
            if (arch.equals("x86"))
            {
                NativeLibraryLoader.load("/net/quasardb/qdb/jni/windows/x86_32/qdb_api.dll");
                NativeLibraryLoader.load("/net/quasardb/qdb/jni/windows/x86_32/qdb_api_jni.dll");
            }
            else
            {
                NativeLibraryLoader.load("/net/quasardb/qdb/jni/windows/x86_64/qdb_api.dll");
                NativeLibraryLoader.load("/net/quasardb/qdb/jni/windows/x86_64/qdb_api_jni.dll");
            }
        }
        else if (os.startsWith("Mac OS X"))
        {
            NativeLibraryLoader.load("/net/quasardb/qdb/jni/osx/x86_64/libqdb_api.dylib");
            NativeLibraryLoader.load("/net/quasardb/qdb/jni/osx/x86_64/libqdb_api_jni.dylib");
        }
        else if (os.startsWith("Linux"))
        {
            NativeLibraryLoader.load("/net/quasardb/qdb/jni/linux/x86_64/libqdb_api.so");
            NativeLibraryLoader.load("/net/quasardb/qdb/jni/linux/x86_64/libqdb_api_jni.so");
        }
        else if (os.startsWith("FreeBSD"))
        {
            NativeLibraryLoader.load("/net/quasardb/qdb/jni/freebsd/x86_64/libqdb_api.so");
            NativeLibraryLoader.load("/net/quasardb/qdb/jni/freebsd/x86_64/libqdb_api_jni.so");
        }
        else
        {
            throw new RuntimeException("Unsupported operating system: " + os);
        }
    }

    public static native String build();
    public static native String version();
    public static native String error_message(int code);

    public static native long open_tcp();
    public static native int connect(long handle, String uri);
    public static native int secure_connect(long handle, String uri, qdb_cluster_security_options securityOptions);
    public static native int close(long handle);
    public static native void release(long handle, ByteBuffer buffer);

    /**
     * Converts a java byte[] and returns an off-heap qdb_string_t *
     */
    public static native long qdb_string_from_bytes(byte[] xs);
    public static native void release_qdb_string(long ptr);

    public static native int option_set_timeout(long handle, int millis);
    public static native int option_set_client_max_in_buf_size(long handle, long size);
    public static native long option_get_client_max_in_buf_size(long handle);
    public static native int option_set_client_max_parallelism(long handle, long threadCount);
    public static native long option_get_client_max_parallelism(long handle);

    public static native int option_set_client_soft_memory_limit(long handle, long threadCount);
    public static native String option_get_client_memory_info(long handle);
    public static native int option_client_tidy_memory(long handle);

    public static native int purge_all(long handle, int timeout);
    public static native int trim_all(long handle, int timeout);
    public static native int wait_for_stabilization(long handle, int timeout);

    public static native int remove(long handle, String alias);
    public static native int expires_at(long handle, String alias, long expiry_time);
    public static native int get_expiry_time(long handle, String alias, Reference<Long> expiry);
    public static native int get_type(long handle, String alias, Reference<Integer> type);
    public static native int get_metadata(long handle, String alias, ByteBuffer metadata);
    public static native boolean entry_exists(long handle, String alias);

    public static native int blob_compare_and_swap(long handle,
                                                   String alias,
                                                   ByteBuffer newContent,
                                                   ByteBuffer comparand,
                                                   long expiry,
                                                   Reference<ByteBuffer> originalContent);
    public static native int blob_put(long handle, String alias, ByteBuffer content, long expiry);
    public static native int blob_get(long handle, String alias, Reference<ByteBuffer> content);
    public static native int blob_get_and_remove(long handle, String alias, Reference<ByteBuffer> content);
    public static native int blob_get_and_update(
        long handle, String alias, ByteBuffer newContent, long expiry, Reference<ByteBuffer> originalContent);
    public static native int blob_remove_if(long handle, String alias, ByteBuffer comparand);
    public static native int blob_update(long handle, String alias, ByteBuffer content, long expiry);

    public static native void timestamp_put(long handle, String alias, Timespec value);
    public static native Timespec timestamp_get(long handle, String alias);
    public static native boolean timestamp_update(long handle, String alias, Timespec value);

    public static native void string_put(long handle, String alias, String content);
    public static native String string_get(long handle, String alias);
    public static native boolean string_update(long handle, String alias, String content);

    public static native void double_put(long handle, String alias, double content);
    public static native double double_get(long handle, String alias);
    public static native boolean double_update(long handle, String alias, double content);

    public static native int int_put(long handle, String alias, long value, long expiry);
    public static native int int_update(long handle, String alias, long value, long expiry);
    public static native int int_get(long handle, String alias, Reference<Long> value);
    public static native int int_add(long handle, String alias, long addend, Reference<Long> result);

    public static native int enable_performance_trace(long handle);
    public static native int disable_performance_trace(long handle);
    public static native int get_performance_traces(long handle, Reference<PerformanceTrace.Trace[]> traces);
    public static native int clear_performance_traces(long handle);

    public static native int attach_tag(long handle, String alias, String tag);
    public static native int has_tag(long handle, String alias, String tag);
    public static native int detach_tag(long handle, String alias, String tag);
    public static native int get_tags(long handle, String alias, Reference<String[]> tags);
    public static native int tag_iterator_begin(long handle, String tag, Reference<Long> iterator);
    public static native int tag_iterator_next(long iterator);
    public static native int tag_iterator_close(long iterator);
    public static native String tag_iterator_alias(long iterator);
    public static native int tag_iterator_type(long iterator);

    public static native int ts_create(long handle, String alias, long shard_size, Column[] columns);
    public static native int ts_remove(long handle, String alias);
    public static native long ts_shard_size(long handle, String alias);

    public static native int ts_insert_columns(long handle, String alias, Column[] columns);
    public static native Column[] ts_list_columns(long handle, String alias);

    // Returns qdb_ts_table_t
    public static native long ts_local_table_init(long handle,
                                                  String alias,
                                                  Column[] columns);


    // Returns qdb_ts_batch_table_t
    public static native void ts_batch_table_release(long handle, long batchTable);

    public static native int ts_batch_push(long handle, long batchTable);
    public static native int ts_batch_push_async(long handle, long batchTable);
    public static native int ts_batch_push_fast(long handle, long batchTable);

    public static native int ts_batch_release_columns_memory(long handle, long batchTable);

    public static native int ts_batch_push_truncate(long handle, long batchTable, TimeRange[] ranges);

    public static native int ts_batch_start_row(long timestamp,
                                                long sec,
                                                long nsec);

    public static native int ts_batch_row_set_double(long batchTable,
                                                     long index,
                                                     double value);
    public static native int ts_batch_row_set_int64(long batchTable,
                                                    long index,
                                                    long value);
    public static native int ts_batch_row_set_timestamp(long batchTable,
                                                        long index,
                                                        long sec,
                                                        long nsec);
    public static native int ts_batch_row_set_blob(long batchTable,
                                                   long index,
                                                   ByteBuffer value);
    public static native int ts_batch_row_set_string(long batchTable,
                                                     long index,
                                                     byte[] value);

    /**
     * Allocates all data structures in one big allocation. For each table, a rowCount
     * and a columnCount is expected.
     *
     * rowCount.length == columnCount.length, and pretty much defines the number of tables.
     */
    public static native long ts_exp_batch_prepare(long handle,
                                                   long[] rowCount,
                                                   long[] columnCount);

    public static native void ts_exp_batch_set_column_from_double(long handle,
                                                                  long batchTables,
                                                                  long tableNum,
                                                                  long columnNum,
                                                                  String name,
                                                                  double[] values);

    public static native void ts_exp_batch_set_column_from_int64(long handle,
                                                                 long batchTables,
                                                                 long tableNum,
                                                                 long columnNum,
                                                                 String name,
                                                                 long[] values);

    public static native void ts_exp_batch_set_column_from_blob(long handle,
                                                                long batchTables,
                                                                long tableNum,
                                                                long columnNum,
                                                                String name,
                                                                ByteBuffer[] values);

    public static native void ts_exp_batch_set_column_from_string(long handle,
                                                                  long batchTables,
                                                                  long tableNum,
                                                                  long columnNum,
                                                                  String name,
                                                                  ByteBuffer[] values);

    public static native void ts_exp_batch_set_column_from_timestamp(long handle,
                                                                     long batchTables,
                                                                     long tableNum,
                                                                     long columnNum,
                                                                     String name,
                                                                     Timespecs values);

    public static native void ts_exp_batch_set_table_data(long handle,
                                                          long batchTables,
                                                          long tableNum,
                                                          String tableName,
                                                          Timespecs timespecs);

    public static native void ts_exp_batch_table_set_drop_duplicates(long batchTables,
                                                                     long tableNum);

    public static native void ts_exp_batch_table_set_drop_duplicate_columns(long handle,
                                                                             long batchTables,
                                                                            long tableNum,
                                                                            String[] columns);

    public static native void ts_exp_batch_table_set_truncate_ranges(long handle,
                                                                     long batchTables,
                                                                     long tableNum,
                                                                     TimeRange[] ranges);

    public static native long ts_exp_batch_push(long handle,
                                                int pushMode,
                                                long batchTables,
                                                long tableCount);


    public static native void ts_exp_batch_release(long handle,
                                                   long batchTables,
                                                   long tableCount);


    // arg: qdb_timespec_t *
    public static native void ts_exp_batch_timestamps_release(long xs);

    public static native void ts_local_table_release(long handle, long localTable);
    public static native int ts_table_get_ranges(long handle, long localTable, TimeRange[] ranges);
    public static native WritableRow ts_table_next_row(long handle, long localTable, Column[] columns);


    public static native Points.Data ts_point_get_ranges(long handle,
                                                         String tableName,
                                                         String columnName,
                                                         int valueType,
                                                         TimeRange[] ranges);

    public static native int ts_point_insert(long handle,
                                             String tableName,
                                             String columnName,
                                             Timespecs timespecs,
                                             int valueType,
                                             Object values);

    public static native int query_execute(long handle, String query, Reference<Result> result);

    public static native int node_status(long handle, String uri, Reference<String> content);
    public static native int node_config(long handle, String uri, Reference<String> content);
    public static native int node_topology(long handle, String uri, Reference<String> content);
    public static native int node_stop(long handle, String uri, String reason);
    public static native int
    get_location(long handle, String alias, Reference<String> address, Reference<Integer> port);

    public static native long init_batch(long handle, int count);
    public static native void release_batch(long handle, long batch);

    public static native int init_operations(long handle, int count, Reference<Long> batch);
    public static native int delete_batch(long handle, long batch);
    public static native int run_batch(long handle, long batch, int count);

    public static native int commit_batch_fast(long handle, long batch, int count);
    public static native int commit_batch_transactional(long handle, long batch, int count);

    public static native void batch_write_blob_compare_and_swap(
        long batch, int index, String alias, ByteBuffer newContent, ByteBuffer comparand, long expiry);
    public static native void batch_write_blob_get(long batch, int index, String alias);
    public static native void
    batch_write_blob_get_and_update(long batch, int index, String alias, ByteBuffer content, long expiry);
    public static native void
    batch_write_blob_put(long batch, int index, String alias, ByteBuffer content, long expiry);
    public static native void
    batch_write_blob_update(long batch, int index, String alias, ByteBuffer content, long expiry);
    public static native int
    batch_read_blob_compare_and_swap(long handle, long batch, int index, String alias, Reference<ByteBuffer> originalContent);
    public static native int batch_read_blob_get(long handle, long batch, int index, String alias, Reference<ByteBuffer> content);
    public static native int
    batch_read_blob_get_and_update(long handle, long batch, int index, String alias, Reference<ByteBuffer> content);
    public static native int batch_read_blob_put(long handle, long batch, int index, String alias);
    public static native int batch_read_blob_update(long handle, long batch, int index, String alias);
}
