package net.quasardb.qdb.ts;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import java.util.*;
import java.util.stream.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;

/**
 * Experimental, high-performance bulk writer for a QuasarDB timeseries table.
 *
 * Usage of instances of this class is not thread-safe. Use a Writer
 * instance per Thread in multi-threaded situations.
 */
public class ExpWriter extends Writer {

    private static final Logger logger = LoggerFactory.getLogger(ExpWriter.class);

    static class StagedTable {
        private static final int initialCapacity = 1;

        Column[] columns;
        ArrayList<Timespec> timestamps;
        ArrayList<ObjectArrayList<Value> > valuesByColumn;

        StagedTable(Column[] columns) {
            this.columns = columns;
            this.timestamps = new ArrayList<Timespec>(initialCapacity);
            this.valuesByColumn = new ArrayList<ObjectArrayList<Value>>(columns.length);

            for (int i = 0; i < this.columns.length; ++i) {
                this.valuesByColumn.add(new ObjectArrayList<Value>(initialCapacity));
            }
        }

        public long rowCount() {
            return this.timestamps.size();
        }

        public long columnCount() {
            return this.columns.length;
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
                ObjectArrayList<Value> xs = this.valuesByColumn.get(i);

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

        public void toNative(long prepped,
                             int tableNum,
                             int offset) {
            Column c = this.columns[offset];
            ObjectArrayList<Value> xs = this.valuesByColumn.get(offset);

            switch (c.getType()) {
            case DOUBLE:
                qdb.ts_exp_batch_set_column_from_double(prepped,
                                                        tableNum,
                                                        offset,
                                                        c.getName(),
                                                        Values.asPrimitiveDoubleArray(xs));
                break;
            case INT64:
                qdb.ts_exp_batch_set_column_from_int64(prepped,
                                                       tableNum,
                                                       offset,
                                                       c.getName(),
                                                       Values.asPrimitiveInt64Array(xs));
                break;
            case BLOB:
                qdb.ts_exp_batch_set_column_from_blob(prepped,
                                                      tableNum,
                                                      offset,
                                                      c.getName(),
                                                      Values.asPrimitiveBlobArray(xs));
                break;

            case SYMBOL:
                //! FALLTHROUGH
            case STRING:
                qdb.ts_exp_batch_set_column_from_string(prepped,
                                                        tableNum,
                                                        offset,
                                                        c.getName(),
                                                        Values.asPrimitiveStringArray(xs));
                break;
            case TIMESTAMP:
                qdb.ts_exp_batch_set_column_from_timestamp(prepped,
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
        public void toNative(long prepped,
                             int tableNum,
                             String tableName) {
            for (int i = 0; i < this.columns.length; ++i) {
                toNative(prepped, tableNum, i);
            }

            qdb.ts_exp_batch_set_table_data(prepped,
                                            tableNum,
                                            tableName,
                                            Timespecs.ofArray(this.timestamps));
        }

        public void toNative(long prepped,
                             int tableNum,
                             String tableName,
                             TimeRange[] truncateRanges) {
            assert(truncateRanges != null);

            toNative(prepped, tableNum, tableName);

            qdb.ts_exp_batch_table_set_truncate_ranges(prepped,
                                                       tableNum,
                                                       truncateRanges);

        }
    }

    private long prepared;
    private HashMap<String, StagedTable> stagedTables;

    protected ExpWriter(Session session, Table[] tables) {
        super(session, tables);

        this.reset();

    }

    protected ExpWriter(Session session, Table[] tables, Writer.PushMode mode) {
        super(session, tables, mode);

        this.reset();
    }

    private void reset() {
        // We reuse the table offsets
        this.stagedTables = new HashMap<String, StagedTable>(this.tableOffsets.size());
        this.prepared = 0;
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

    @Override
    public void append(Integer offset, Timespec timestamp, Value[] values) throws IOException {
        super.trackMinMaxTimestamp(timestamp);


        StagedTable t = this.getStagedTable(this.tableByIndex(offset.intValue()));

        t.append(timestamp, values);
    }

    public void prepareFlush() throws IOException  {
        this.prepareFlush(null);
    }

    public void prepareFlush(TimeRange[] ranges) {
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

        if (this.pushMode() == PushMode.TRUNCATE) {
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


        this.prepared = qdb.ts_exp_batch_prepare(rowCount,
                                                 columnCount);

        i = 0;
        for (Map.Entry<String, StagedTable> x : this.stagedTables.entrySet()) {
            String tableName = x.getKey();
            StagedTable stagedTable = x.getValue();

            if (truncateRanges[i] == null) {
                stagedTable.toNative(this.prepared, i, tableName);
            } else {
                stagedTable.toNative(this.prepared, i, tableName, truncateRanges[i]);
            }
            i++;
        }
    }

    @Override
    public void flush() throws IOException {
        try {

            if (this.prepared == 0) {
                this.prepareFlush();
            }

            qdb.ts_exp_batch_push(this.session.handle(),
                                  this.pushMode.asInt(),
                                  this.prepared, this.stagedTables.size());
        } finally {
            if (this.prepared != 0) {
                qdb.ts_exp_batch_release(this.prepared, this.stagedTables.size());
            }
            this.prepared = 0;
        }

        this.reset();
    }

    @Override
    public void flush(TimeRange[] ranges) throws IOException {
        this.prepareFlush(ranges);
        this.flush();
    }

}
