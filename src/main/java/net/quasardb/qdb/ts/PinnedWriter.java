package net.quasardb.qdb.ts;

import java.io.IOException;
import java.io.Flushable;
import java.lang.AutoCloseable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import java.nio.channels.SeekableByteChannel;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.InputException;
import net.quasardb.qdb.exception.InvalidArgumentException;
import net.quasardb.qdb.exception.OutOfBoundsException;
import net.quasardb.qdb.jni.*;

/**
 * Experimental, high-performance bulk writer for a QuasarDB timeseries table.
 *
 * Usage of instances of this class is not thread-safe. Use a Writer
 * instance per Thread in multi-threaded situations.
 */
public class PinnedWriter extends Writer {

    private static final Logger logger = LoggerFactory.getLogger(PinnedWriter.class);

    private final int chunkSize = 50000;

    private int currentRow;
    private Long2ObjectOpenHashMap<PinnedMatrix> matrixByShard;
    private Value.Type[] columnTypes;
    private long[] columnShardSizes;

    private Value[][] valuesByColumn;

    public static class PinnedMatrix {
        Value.Type[] columnTypes;
        ArrayList<Long> timeoffsets;
        ArrayList<ArrayList<Value> > valuesByColumn;
        int currentRow;

        public PinnedMatrix(Value.Type[] columnTypes) {

            this.columnTypes = columnTypes;
            this.currentRow = 0;
            this.timeoffsets = new ArrayList<Long>();

            int columnCount = columnTypes.length;
            this.valuesByColumn = new ArrayList<ArrayList <Value> >(columnCount);

            for (int i = 0; i < columnCount; ++i) {
                this.valuesByColumn.add(i, new ArrayList<Value>());
            }

            assert(this.valuesByColumn.size() == columnCount);

        }

        void add (int offset, long timeOffset, Value[] values) {
            this.timeoffsets.add(this.currentRow, timeOffset);
            for (int i = 0; i < values.length; ++i) {
                ArrayList columnValues = this.valuesByColumn.get(offset + i);
                assert(columnValues.size () == this.currentRow);

                columnValues.add(this.currentRow, values[i]);
            }

            ++this.currentRow;

        }

        void flush(long handle, long batchTable, long shard) {
            // `offset` is the relative offset of the column within the total
            // batch writer state.
            assert(this.columnTypes.length == this.valuesByColumn.size());

            long[] timeoffsets = new long[this.timeoffsets.size()];
            for (int i = 0; i < this.timeoffsets.size(); ++i) {
                timeoffsets[i] = this.timeoffsets.get(i).longValue();
            }

            for (int offset = 0; offset < this.valuesByColumn.size(); ++offset) {
                ArrayList<Value> columnValues = valuesByColumn.get(offset);

                if (columnValues.size() == 0) {
                    continue;
                }

                Value.Type columnType = this.columnTypes[offset];
                assert(columnValues != null);

                switch (columnType) {
                case DOUBLE:
                    qdb.ts_batch_set_pinned_doubles(handle,
                                                    batchTable,
                                                    shard,
                                                    offset,
                                                    timeoffsets,
                                                    Values.asPrimitiveDoubleArray(columnValues));
                    break;

                case INT64:
                    qdb.ts_batch_set_pinned_int64s(handle,
                                                   batchTable,
                                                   shard,
                                                   offset,
                                                   timeoffsets,
                                                   Values.asPrimitiveInt64Array(columnValues));
                    break;

                case TIMESTAMP:
                    qdb.ts_batch_set_pinned_timestamps(handle,
                                                       batchTable,
                                                       shard,
                                                       offset,
                                                       timeoffsets,
                                                       Values.asPrimitiveTimestampArray(columnValues));
                    break;

                default:
                    throw new RuntimeException("Column type not yet implemented: " + columnType.toString());
                };
            }

        }
    }


    protected PinnedWriter(Session session, Table[] tables) {
        super(session, tables);
        this.currentRow = 0;
        this.matrixByShard = new Long2ObjectOpenHashMap<PinnedMatrix>();
        this.resolveShardSizes(tables);
        this.resolveColumnTypes(tables);
    }

    protected PinnedWriter(Session session, Table[] tables, Writer.PushMode mode) {
        super(session, tables, mode);
        this.currentRow = 0;
        this.matrixByShard = new Long2ObjectOpenHashMap<PinnedMatrix>();

        this.resolveShardSizes(tables);
        this.resolveColumnTypes(tables);
    }

    private void resolveShardSizes(Table[] tables) {
        // We know in advance the amount of columns we expect.
        this.columnShardSizes = new long[this.columns.size()];

        int i = 0;

        for (Table t : tables) {
            for (Column c : t.columns) {
                this.columnShardSizes[i++] = t.getShardSize();
            }
        }

        assert i == this.columnShardSizes.length;
    }

    private void resolveColumnTypes(Table[] tables) {
        // We know in advance the amount of columns we expect.
        this.columnTypes = new Value.Type[this.columns.size()];

        int i = 0;

        for (Table t : tables) {
            for (Column c : t.columns) {
                this.columnTypes[i++] =  c.getType();
            }
        }

        assert i == this.columnTypes.length;
    }

    private static long[] pinColumns(Long batchTable, List<Writer.TableColumn> columns) {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public void extraTables(Table[] tables) {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public void append(Integer offset, Timespec timestamp, Value[] values) throws IOException {
        assert this.currentRow < this.chunkSize : "Internal row index cannot exceed chunk size";
        long shard = PinnedWriter.truncateTimespecToShard(this.columnShardSizes[offset],
                                                          timestamp.getSec());

        PinnedMatrix xs = this.matrixByShard.get(shard);
        if (xs == null) {

            // We .wrap() the array, because .elements() does returns an Object[] if
            // we don't. Javadoc for elements():
            //
            // "If this array list was created by wrapping a given array, it is guaranteed
            // that the type of the returned array will be the same. Otherwise, the returned
            // array will be of type Object[] (in spite of the declared return type)."
            //
            // http://fastutil.di.unimi.it/docs/it/unimi/dsi/fastutil/objects/ObjectArrayList.html#elements()
            //
            // I don't fully understand the reason why the internal array is represented
            // as Object[] if we don't do this (because what's the point of template
            // classes if it can't, right?), but this is a workable workaround.
            xs = new PinnedMatrix(this.columnTypes);
            this.matrixByShard.put(shard, xs);
        }

        xs.add(offset, this.calculateOffset(shard, timestamp), values);
    }

    @Override
    public void flush() throws IOException {
        for (Long shard : this.matrixByShard.keySet()) {
            PinnedMatrix xs = this.matrixByShard.get(shard);
            assert(xs != null);

            xs.flush(this.session.handle(),
                     this.batchTable,
                     shard);
        }

        super.flush();
    }

    private static long calculateOffset(long shard, Timespec ts) {
        return ts.minusSeconds(shard).toEpochNanos();
    }

    private static long truncateTimespecToShard(long shardSize, long secs) {
        return secs - (secs % shardSize);
    }
}
