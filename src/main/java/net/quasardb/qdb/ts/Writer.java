package net.quasardb.qdb.ts;

import java.io.IOException;
import java.io.Flushable;
import java.lang.AutoCloseable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import java.nio.channels.SeekableByteChannel;
import java.util.*;
import java.util.stream.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.InputException;
import net.quasardb.qdb.exception.InvalidArgumentException;
import net.quasardb.qdb.exception.OutOfBoundsException;
import net.quasardb.qdb.jni.*;

/**
 * High-performance bulk writer for a QuasarDB timeseries table.
 *
 * Usage of instances of this class is not thread-safe. Use a Writer
 * instance per Thread in multi-threaded situations.
 */
public class Writer implements AutoCloseable, Flushable {

    /**
     * Determines which mode of operation to use when flushing the writer.
     */
    public enum PushMode {
        NORMAL(0),
        ASYNC(1),
        FAST(2),
        TRUNCATE(3)

        ;

        protected final int value;

        PushMode(int type) {
            this.value = type;
        }

        public int asInt() {
            return this.value;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(Writer.class);

    static class StagedTable {
        private static final int initialCapacity = 1;

        Column[] columns;
        ArrayList<Timespec> timestamps;
        ArrayList<ArrayList<Value> > valuesByColumn;

        StagedTable(Column[] columns) {
            this.columns = columns;
            this.timestamps = new ArrayList<Timespec>(initialCapacity);
            this.valuesByColumn = new ArrayList<ArrayList<Value>>(columns.length);

            for (int i = 0; i < this.columns.length; ++i) {
                this.valuesByColumn.add(new ArrayList<Value>(initialCapacity));
            }
        }

        public long rowCount() {
            return this.timestamps.size();
        }

        public long columnCount() {
            return this.columns.length;
        }

        public long valueCount() {
            return rowCount() * columnCount();
        }

        /**
         * As we are receiving the data in row-oriented fashion, while appending
         * we pre-pivot the dataset so that we store everything in column-oriented
         * fashion. This will ensure that later, we can easily convert all arrays.
         */
        public void append(Timespec timestamp, Value[] values) {
            this.timestamps.add(timestamp);

            // For now, we require values for every column
            assert(values.length == this.columns.length);

            for (int i = 0; i < values.length; ++i) {
                ArrayList<Value> xs = this.valuesByColumn.get(i);

                if (values[i].getType() == Value.Type.STRING) {
                    // TODO(leon): once the new writer have stabilized, we should
                    // always represent all strings as bytebuffers immeidately.
                    //
                    // Invoking this call here has the advantage that the Value matrix
                    // we buffer 'owns' the directly allocated memory region, and means
                    // it's released automatically when the GC decides the Value objects
                    // (and thus, their underlying direct ByteBuffers) are to be evicted.
                    values[i].ensureByteBufferBackedString();
                }

                xs.add(values[i]);

                // Sanity check: all timestamps + column values arrays are of equal size.
                assert(xs.size() == this.timestamps.size());
            }
        }

        public void toNative(long handle,
                             long prepped,
                             int tableNum,
                             int offset) {
            Column c = this.columns[offset];
            ArrayList<Value> xs = this.valuesByColumn.get(offset);

            switch (c.getType()) {
            case DOUBLE:
                qdb.ts_exp_batch_set_column_from_double(handle,
                                                        prepped,
                                                        tableNum,
                                                        offset,
                                                        c.getName(),
                                                        Values.asPrimitiveDoubleArray(xs));
                break;
            case INT64:
                qdb.ts_exp_batch_set_column_from_int64(handle,
                                                       prepped,
                                                       tableNum,
                                                       offset,
                                                       c.getName(),
                                                       Values.asPrimitiveInt64Array(xs));
                break;
            case BLOB:
                qdb.ts_exp_batch_set_column_from_blob(handle,
                                                      prepped,
                                                      tableNum,
                                                      offset,
                                                      c.getName(),
                                                      Values.asPrimitiveBlobArray(xs));
                break;

            case SYMBOL:
                //! FALLTHROUGH
            case STRING:
                qdb.ts_exp_batch_set_column_from_string(handle,
                                                        prepped,
                                                        tableNum,
                                                        offset,
                                                        c.getName(),
                                                        Values.asPrimitiveStringArray(xs));
                break;
            case TIMESTAMP:
                qdb.ts_exp_batch_set_column_from_timestamp(handle,
                                                           prepped,
                                                           tableNum,
                                                           offset,
                                                           c.getName(),
                                                           Values.asPrimitiveTimestampArray(xs));
                break;
            default:
                throw new RuntimeException("Unrecognized column type: " + c.toString());
            };
        }

        /**
         * qdb
         */
        public void toNative(long handle,
                             long prepped,
                             int tableNum,
                             String tableName,
                             Options options) {
            for (int i = 0; i < this.columns.length; ++i) {
                toNative(handle, prepped, tableNum, i);
            }

            qdb.ts_exp_batch_set_table_data(handle,
                                            prepped,
                                            tableNum,
                                            tableName,
                                            Timespecs.ofArray(this.timestamps));


            if (options.isDropDuplicatesEnabled() == true) {
                logger.debug("enabling deduplication while flushing to table {}", tableName);
                qdb.ts_exp_batch_table_set_drop_duplicates(prepped, tableNum);

                if (options.hasDropDuplicateColumns() == true) {
                    logger.debug("enabling column-wise deduplication while flushing to table {}", tableName);
                    qdb.ts_exp_batch_table_set_drop_duplicate_columns(handle,
                                                                      prepped,
                                                                      tableNum,
                                                                      options.getDropDuplicateColumns());
                };
            };
        }

        public void toNative(long handle,
                             long prepped,
                             int tableNum,
                             String tableName,
                             Options options,
                             TimeRange[] truncateRanges) {
            assert(truncateRanges != null);

            toNative(handle, prepped, tableNum, tableName, options);

            qdb.ts_exp_batch_table_set_truncate_ranges(handle,
                                                       prepped,
                                                       tableNum,
                                                       truncateRanges);

        }
    }



    /**
     * Batch writer options.
     */
    static public class Options {
        private PushMode pushMode;
        private boolean dropDuplicates;
        private String[] dropDuplicateColumns;

        public Options() {
            this.pushMode = PushMode.NORMAL;
            this.dropDuplicates = false;
            this.dropDuplicateColumns = null;
        };

        /**
         * Resets push mode to 'normal'.
         */
        public void enableNormalPush() {
            this.pushMode = PushMode.NORMAL;
        };

        /**
         * Sets push mode to 'fast'.
         */
        public void enableFastPush() {
            this.pushMode = PushMode.FAST;
        };

        /**
         * Sets push mode to 'async'.
         */
        public void enableAsyncPush() {
            this.pushMode = PushMode.ASYNC;
        };

        /**
         * Sets push mode to 'truncate'.
         */
        public void enableTruncatePush() {
            this.pushMode = PushMode.TRUNCATE;
        };

        /**
         * Get the currently set push mode.
         */
        public PushMode getPushMode() {
            return this.pushMode;
        };

        /**
         * Enables server-side deduplication when all values of a row
         * match.
         */
        public void enableDropDuplicates() {
            this.dropDuplicates = true;
        };

        /**
         * Enables server-side deduplication when values of provided columns
         * match.
         */
        public void enableDropDuplicates(String[] columns) {
            this.dropDuplicates = true;
            this.dropDuplicateColumns = columns;
        };

        /**
         * Enables server-side deduplication when values of provided columns
         * match.
         */
        public void enableDropDuplicates(Column[] columns) {
            String[] columnNames = new String[columns.length];

            for (int i = 0; i < columns.length; ++i) {
                columnNames[i] = columns[i].getName();
            };

            enableDropDuplicates(columnNames);
        };

        /**
         * Disables server-side deduplication.
         */
        public void disableDropDuplicates() {
            this.dropDuplicates = false;
            this.dropDuplicateColumns = null;
        };

        /**
         * Returns true if server-side deduplication is enabled.
         */
        public boolean isDropDuplicatesEnabled() {
            return this.dropDuplicates;
        };

        /**
         * Returns true if column-wise server-side deduplication is enabled.
         */
        public boolean hasDropDuplicateColumns() {
            return this.dropDuplicateColumns != null;
        };

        /**
         * Returns the columns to perform server-side deduplication on.
         */
        public String[] getDropDuplicateColumns() {
            assert(this.isDropDuplicatesEnabled() == true);
            assert(this.hasDropDuplicateColumns() == true);

            return this.dropDuplicateColumns;
        };
    };


    private Options options;
    private long prepared = 0;
    private HashMap<String, StagedTable> stagedTables;

    protected long pointsSinceFlush = 0;
    Session session;

    TimeRange minMaxTs;

    protected Writer(Session session, Options options) {
        this.session = session;
        this.options = options;

        this.minMaxTs = null;

        this.reset();

        logger.info("Successfully initialized Writer");
    }

    /**
     * Create a builder instance.
     *
     * @param session Active connection with the QuasarDB cluster.
     */
    public static Builder builder(Session session) {
        return new Builder(session);
    };

    private void reset() {
        logger.debug("resetting internal batch writer state");

        if (this.prepared != 0) {
            logger.info("releasing batch writer state");
            qdb.ts_exp_batch_release(this.session.handle(),
                                     this.prepared, this.stagedTables.size());
            this.prepared = 0;
        }

        this.stagedTables = new HashMap<String, StagedTable>();

        this.pointsSinceFlush = 0;
        this.minMaxTs = null;
    }

    private StagedTable getStagedTable(Table t) {
        String name = t.getName();
        StagedTable ret = this.stagedTables.get(name);
        if (ret == null) {
            this.stagedTables.put(name, new StagedTable(t.getColumns()));
            ret = this.stagedTables.get(name);
        }

        assert(ret != null);

        return ret;
    }

    /**
     * Cleans up the internal representation of the batch table.
     */
    @Override
    protected void finalize() throws Throwable {
        this.reset();
    }

    /**
     * Closes the timeseries table and local cache so that memory can be reclaimed. Flushes
     * all remaining output.
     */
    public void close() throws IOException {
        this.reset();
    }

    public void flush() throws IOException {
        try {

            if (this.prepared == 0) {
                this.prepareFlush();
            }

            if (this.prepared == 0) {
                logger.warn("Unable to prepare flush, skipping...");
                return;
            }

            logger.info("Flushing batch writer, push mode='{}', points since last flush={}", this.options.getPushMode().toString(), this.pointsSinceFlush);
            qdb.ts_exp_batch_push(this.session.handle(),
                                  this.options.getPushMode().asInt(),
                                  this.prepared,
                                  this.stagedTables.size());
        } finally {
            this.reset();
            assert(this.prepared == 0);
        }
    }

    public void flush(TimeRange[] ranges) throws IOException {
        this.prepareFlush(ranges);
        this.flush();
    }

    /**
     * Prepare internal data structure for flushing. Will be automatically called if
     * not called explicitly.
     */
    public void prepareFlush() throws IOException  {
        this.prepareFlush(null);
    }

    /**
     * Prepare internal data structure for flushing. Will be automatically called if
     * not called explicitly.
     */
    public void prepareFlush(TimeRange[] ranges) {
        // Logic below and internally within the C++ parts assumes that we have
        // at least 1 table to flush.
        if (this.stagedTables.size() == 0) {
            logger.warn("No tables staged, nothing to flush!");
            assert(this.prepared == 0);
            return;
        }

        long[]        rowCount       = new long[this.stagedTables.size()];
        long[]        columnCount    = new long[this.stagedTables.size()];

        int i = 0;
        for (StagedTable t : this.stagedTables.values()) {
            columnCount[i] = t.columnCount();
            rowCount[i] = t.rowCount();

            ++i;
        }

        // The data structure / logic below is set up to handle different ranges
        // per table, but we don't actually support this on a high-level yet.
        TimeRange[][] truncateRanges = new TimeRange[this.stagedTables.size()][];

        if (this.options.getPushMode() == PushMode.TRUNCATE) {
            if (ranges == null && this.minMaxTs != null) {
                ranges = new TimeRange[] {
                    this.minMaxTs.withEnd(this.minMaxTs.end.plusNanos(1))
                };
            }

            // As mentioned above, all ranges are the same for all tables. We *could*
            // actually do this on a per-table basis.
            Arrays.fill(truncateRanges, ranges);
        } else {
            if (ranges != null) {
                logger.warn("Truncate ranges provided but insert mode is not truncate!");
            }

            // A 'null' value for the truncate ranges is interpreted as 'no range' by
            // the C++ code.
            Arrays.fill(truncateRanges, null);
        }


        this.prepared = qdb.ts_exp_batch_prepare(this.session.handle(),
                                                 rowCount,
                                                 columnCount);

        i = 0;
        for (Map.Entry<String, StagedTable> x : this.stagedTables.entrySet()) {
            String tableName = x.getKey();
            StagedTable stagedTable = x.getValue();

            if (truncateRanges[i] == null) {
                stagedTable.toNative(this.session.handle(),
                                     this.prepared, i, tableName, this.options);
            } else {
                stagedTable.toNative(this.session.handle(),
                                     this.prepared, i, tableName, this.options, truncateRanges[i]);
            }
            i++;
        }
    }

    protected void trackMinMaxTimestamp(Timespec timestamp) {
        if (this.minMaxTs == null) {
            this.minMaxTs = new TimeRange(timestamp, timestamp);
        } else {
            this.minMaxTs = TimeRange.merge(this.minMaxTs, timestamp);
        }
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * This function automatically looks up a table's offset by its name. For performance
     * reason, you are encouraged to manually invoke and cache the value of #tableIndexByName
     * whenever possible.
     *
     * @param table Table to insert into.
     * @param timestamp Timestamp of the row
     * @param values Values being inserted, mapped to columns by their relative offset.
     *
     * @see #flush
     */
    public void append(Table table, Timespec timestamp, Value[] values) throws IOException {
        this.trackMinMaxTimestamp(timestamp);
        this.pointsSinceFlush += values.length;

        StagedTable t = this.getStagedTable(table);

        t.append(timestamp, values);
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * @param table Table to insert into
     * @param row Row being inserted.
     *
     * @see #flush
     */
    public void append(Table table, WritableRow row) throws IOException {
        this.append(table,
                    row.getTimestamp(),
                    row.getValues());
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * @param table Table to insert into
     * @param timestamp Timestamp of the row
     * @param values Values being inserted, mapped to columns by their relative offset.
     *
     * @see #flush
     */
    public void append(Table table, LocalDateTime timestamp, Value[] values) throws IOException {
        this.append(table,
                    new Timespec(timestamp),
                    values);
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * @param table Table to insert into
     * @param timestamp Timestamp of the row
     * @param values Values being inserted, mapped to columns by their relative offset.
     *
     * @see #flush
     */
    public void append(Table table, Timestamp timestamp, Value[] values) throws IOException {
        this.append(table,
                    new Timespec(timestamp),
                    values);
    }

    /**
     * Returns the amount of values appended to the writer, not yet pushed/flushed.
     */
    public long size() {
        long n = 0;

        for (StagedTable t : this.stagedTables.values()) {
            n += t.valueCount();
        }

        return n;
    }

    public static final class Builder {
        private Session session;
        private Writer.Options options;

        protected Builder(Session session) {
            this.session = session;
            this.options = new Writer.Options();
        };

        public Builder normalPush() {
            this.options.enableNormalPush();
            return this;
        };

        public Builder fastPush() {
            this.options.enableFastPush();
            return this;
        };

        public Builder asyncPush() {
            this.options.enableAsyncPush();
            return this;
        };

        public Builder truncatePush() {
            this.options.enableTruncatePush();
            return this;
        };

        public Builder dropDuplicates() {
            this.options.enableDropDuplicates();
            return this;
        };

        public Builder dropDuplicates(String[] columns) {
            this.options.enableDropDuplicates(columns);
            return this;
        };

        public Builder dropDuplicates(Column[] columns) {
            this.options.enableDropDuplicates(columns);
            return this;
        };

        public Writer build() {
            return new Writer(this.session, this.options);
        };

    };

}
