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
public class PinnedWriter extends Writer {

    private static final Logger logger = LoggerFactory.getLogger(PinnedWriter.class);

    private boolean pinned;
    private Int2ObjectLinkedOpenHashMap<Long2ObjectOpenHashMap<PinnedMatrix>> shardsByTableOffset;
    private Int2ObjectLinkedOpenHashMap<Value.Type[]> columnTypesByTableOffset;

    private long[] columnShardSizes;

    private Value[][] valuesByColumn;

    public static class PinnedMatrix {
        Value.Type[] columnTypes;
        LongArrayList timeoffsets;
        ObjectArrayList<Value>[] valuesByColumn;
        int currentRow;

        public PinnedMatrix(Value.Type[] columnTypes) {

            this.columnTypes = columnTypes;
            this.currentRow = 0;
            this.timeoffsets = new LongArrayList(64);


            this.valuesByColumn = PinnedMatrix.toValuesByColumn(columnTypes);
            assert(this.valuesByColumn.length == columnTypes.length);
        }

        private static ObjectArrayList<Value>[] toValuesByColumn(Value.Type[] columnTypes) {
            int columnCount = columnTypes.length;
            ObjectArrayList<Value>[] result = new ObjectArrayList[columnCount];

            for (int i = 0; i < columnCount; ++i) {
                result[i] = new ObjectArrayList<Value>(64);
            }

            return result;
        }

        void add (long timeOffset, Value[] values) {
            // Just a basic sanity check
            assert(this.valuesByColumn.length == columnTypes.length);

            // We expect a value for each column
            assert(values.length == this.valuesByColumn.length);

            this.timeoffsets.add(timeOffset);
            for (int i = 0; i < values.length; ++i) {
                ObjectArrayList<Value> columnValues = this.valuesByColumn[i];
                assert(columnValues != null);

                if (values[i].getType() == Value.Type.STRING) {
                    // TODO(leon): once pinned writers have stabilized, we should
                    // always represent all strings as bytebuffers immeidately.
                    //
                    // Invoking this call here has the advantage that the Value matrix
                    // we buffer 'owns' the directly allocated memory region, and means
                    // it's released automatically when the GC decides the Value objects
                    // (and thus, their underlying direct ByteBuffers) are to be evicted.
                    values[i].ensureByteBufferBackedString();
                }

                columnValues.add(values[i]);

                assert(columnValues.size() == this.timeoffsets.size());
            }
        }

        void flush(long handle, long batchTable, long shard, int tableOffset) {
            // Since every row is guaranteed to contain a time offset, we can use
            // the timeoffsets to determine the 'height' of the matrix.
            //
            // We'll then make a 'perfect' matrix, and fill any non-set values in
            // any column with an explicit null value.
            for (int columnOffset = 0; columnOffset < this.columnTypes.length; ++columnOffset) {

                ObjectArrayList<Value> columnValues = this.valuesByColumn[columnOffset];
                Value.Type columnType = this.columnTypes[columnOffset];

                assert(columnValues != null);
                assert(columnType != null);
                assert(columnValues.size() == this.timeoffsets.size());

                long[] timeoffsets_ = this.timeoffsets.toArray(new long[this.timeoffsets.size()]);

                logger.debug("pinning column with offset {}, type {} and length {}", columnOffset, columnType, columnValues.size());

                switch (columnType) {
                case DOUBLE:
                    {
                        double[] xs = Values.asPrimitiveDoubleArray(columnValues);
                        assert(xs.length == timeoffsets_.length);
                        qdb.ts_batch_set_pinned_doubles(handle,
                                                        batchTable,
                                                        shard,
                                                        tableOffset + columnOffset,
                                                        timeoffsets_,
                                                        xs);
                        break;
                    }

                case INT64:
                    qdb.ts_batch_set_pinned_int64s(handle,
                                                   batchTable,
                                                   shard,
                                                   tableOffset + columnOffset,
                                                   timeoffsets_,
                                                   Values.asPrimitiveInt64Array(columnValues));
                    break;

                case TIMESTAMP:
                    qdb.ts_batch_set_pinned_timestamps(handle,
                                                       batchTable,
                                                       shard,
                                                       tableOffset + columnOffset,
                                                       timeoffsets_,
                                                       Values.asPrimitiveTimestampArray(columnValues));
                    break;

                case BLOB:
                    qdb.ts_batch_set_pinned_blobs(handle,
                                                  batchTable,
                                                  shard,
                                                  tableOffset + columnOffset,
                                                  timeoffsets_,
                                                  Values.asPrimitiveBlobArray(columnValues));
                    break;

                case STRING:
                    qdb.ts_batch_set_pinned_strings(handle,
                                                    batchTable,
                                                    shard,
                                                    tableOffset + columnOffset,
                                                    timeoffsets_,
                                                    Values.asPrimitiveStringArray(columnValues));
                    break;

                default:
                    throw new RuntimeException("Column type not yet implemented: " + columnType.toString());
                };
            }
        }
    }


    protected PinnedWriter(Session session, Table[] tables) {
        super(session, tables);
        this.pinned = false;
        this.columnTypesByTableOffset = new Int2ObjectLinkedOpenHashMap<Value.Type[]>();
        this.shardsByTableOffset = new Int2ObjectLinkedOpenHashMap<Long2ObjectOpenHashMap<PinnedMatrix>>();

        this.indexTables(tables);

        assert(this.columnShardSizes != null);
        assert(this.columnTypesByTableOffset.size () == tables.length);
    }

    protected PinnedWriter(Session session, Table[] tables, Writer.PushMode mode) {
        super(session, tables, mode);
        this.pinned = false;
        this.columnTypesByTableOffset = new Int2ObjectLinkedOpenHashMap<Value.Type[]>();
        this.shardsByTableOffset = new Int2ObjectLinkedOpenHashMap<Long2ObjectOpenHashMap<PinnedMatrix>>();

        this.indexTables(tables);

        assert(this.columnShardSizes != null);
        assert(this.columnTypesByTableOffset.size () == tables.length);
    }

    private static long[] tablesToColumnShardSizes(Table[] tables) {
        int columnCount =
            Arrays.stream(tables).map(t -> t.getColumns().length).reduce(0, Integer::sum);
        long[] result = new long[columnCount];

        int i = 0;

        for (Table t : tables) {
            for (Column c : t.columns) {
                result[i++] = t.getShardSize();
            }
        }

        return result;
    }

    private static Value.Type[] tablesToColumnTypes(Table[] tables) {
        int columnCount =
            Arrays.stream(tables).map(t -> t.getColumns().length).reduce(0, Integer::sum);
        Value.Type[] result = new Value.Type[columnCount];

        int i = 0;

        for (Table t : tables) {
            for (Column c : t.columns) {
                result[i++] = c.getType();
            }
        }

        return result;
    }

    private void indexTables(Table[] tables) {

        long[] newShardSizes = PinnedWriter.tablesToColumnShardSizes(tables);

        if (this.columnShardSizes == null) {
            this.columnShardSizes = newShardSizes;
        } else {
            this.columnShardSizes = LongStream
                .concat(Arrays.stream(this.columnShardSizes),
                        Arrays.stream(newShardSizes))
                .toArray();
        }

        for (Table table : tables) {
            int index = this.tableIndexByName(table.getName());
            this.columnTypesByTableOffset.put(index, table.getColumnTypes());
        }
    }

    @Override
    public void extraTables(Table[] tables) {
        super.extraTables(tables);
        this.indexTables(tables);
    }

    @Override
    public void append(Integer offset, Timespec timestamp, Value[] values) throws IOException {
        super.trackMinMaxTimestamp(timestamp);
        long shard = PinnedWriter.truncateTimespecToShard(this.columnShardSizes[offset],
                                                          timestamp.getSec());

        Long2ObjectOpenHashMap<PinnedMatrix> shards = this.shardsByTableOffset.get(offset);
        if (shards == null) {
            shards = new Long2ObjectOpenHashMap<PinnedMatrix>();
            this.shardsByTableOffset.put(offset, shards);
        }

        PinnedMatrix xs = shards.get(shard);
        if (xs == null) {
            Value.Type[] columnTypes = this.columnTypesByTableOffset.get(offset);
            assert(columnTypes != null);
            xs = new PinnedMatrix(columnTypes);
            shards.put(shard, xs);
        }

        xs.add(this.calculateOffset(shard, timestamp), values);

        this.pointsSinceFlush += values.length;
    }

    public void prepareFlush() throws IOException  {
        Instant startPinTime = Instant.now();

        IntSortedSet tableOffsets = this.shardsByTableOffset.keySet();
        IntBidirectionalIterator iter = tableOffsets.iterator();

        while (iter.hasNext()) {
            int tableOffset = iter.nextInt();
            Long2ObjectOpenHashMap<PinnedMatrix> shards = this.shardsByTableOffset.get(tableOffset);

            LongSet shardIds = shards.keySet();
            LongIterator iter_ = shardIds.iterator();
            while (iter_.hasNext()) {
                long shardId = iter_.nextLong();

                PinnedMatrix xs = shards.get(shardId);
                assert(xs != null);

                xs.flush(this.session.handle(),
                         this.batchTable,
                         shardId,
                         tableOffset);

            }
        }

        this.pinned = true;
    }

    @Override
    public void flush() throws IOException {
        if (this.pinned == false) {
            this.prepareFlush();
        }

        Instant startFlushTime = Instant.now();

        super.flush();

        Instant endFlushTime = Instant.now();

        logger.debug("Columns flushed in {}", Duration.between(startFlushTime, endFlushTime));


        // Reset our internal state so that we are ready to pin the next batch of rows.
        // We could probably re-use existing matrixes for more efficient memory management,
        // but it's simpler to just reset it.
        this.shardsByTableOffset = new Int2ObjectLinkedOpenHashMap<Long2ObjectOpenHashMap<PinnedMatrix>>();
        this.pinned = false;
        qdb.ts_batch_release_columns_memory(this.session.handle(),
                                            this.batchTable);
    }

    private static long calculateOffset(long shard, Timespec ts) {
        return ts.minusSeconds(shard).toEpochNanos();
    }

    private static long truncateTimespecToShard(long shardSize, long secs) {
        return secs - (secs % shardSize);
    }
}
