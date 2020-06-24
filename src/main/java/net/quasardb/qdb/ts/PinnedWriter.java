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
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
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

    private int currentRow;
    private Long2ObjectOpenHashMap<PinnedMatrix> matrixByShard;
    private Value.Type[] columnTypes;
    private long[] columnShardSizes;

    private Value[][] valuesByColumn;

    public static class PinnedMatrix {
        Value.Type[] columnTypes;
        LongArrayList timeoffsets;
        ArrayList<Int2ObjectLinkedOpenHashMap<Value> > valuesByColumn;
        int currentRow;

        public PinnedMatrix(Value.Type[] columnTypes) {

            this.columnTypes = columnTypes;
            this.currentRow = 0;
            this.timeoffsets = new LongArrayList(64 * 1024); // 64k values pre-allocated

            this.valuesByColumn = PinnedMatrix.toValuesByColumn(columnTypes);
            assert(this.valuesByColumn.size() == columnTypes.length);
        }

        private static ArrayList<Int2ObjectLinkedOpenHashMap<Value> > toValuesByColumn(Value.Type[] columnTypes) {
            int columnCount = columnTypes.length;
            ArrayList<Int2ObjectLinkedOpenHashMap<Value> > result = new ArrayList<Int2ObjectLinkedOpenHashMap<Value> >(columnCount);

            for (int i = 0; i < columnCount; ++i) {
                result.add(i, null);
            }

            return result;
        }

        void extraColumns(Value.Type[] columnTypes) {
            assert(this.valuesByColumn != null);

            this.valuesByColumn.addAll(PinnedMatrix.toValuesByColumn(columnTypes));

            this.columnTypes = Stream
                .concat(Arrays.stream(this.columnTypes),
                        Arrays.stream(columnTypes))
                .toArray(Value.Type[]::new);

        }

        void add (int offset, long timeOffset, Value[] values) {
            this.timeoffsets.add(this.currentRow, timeOffset);
            for (int i = 0; i < values.length; ++i) {
                Int2ObjectLinkedOpenHashMap<Value> columnValues = this.valuesByColumn.get(offset + i);

                if (columnValues == null) {
                    columnValues = new Int2ObjectLinkedOpenHashMap<Value>();
                    this.valuesByColumn.add(offset + i, columnValues);
                }

                assert(columnValues.containsKey(this.currentRow) == false);

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

                columnValues.put(this.currentRow, values[i]);
            }

            ++this.currentRow;
        }

        void flush(long handle, long batchTable, long shard) {
            // Since every row is guaranteed to contain a time offset, we can use
            // the timeoffsets to determine the 'height' of the matrix.
            //
            // We'll then make a 'perfect' matrix, and fill any non-set values in
            // any column with an explicit null value.
            int matrixHeight = this.timeoffsets.size();
            assert(this.timeoffsets.size() == this.currentRow);

            for (int offset = 0; offset < this.columnTypes.length; ++offset) {

                Int2ObjectLinkedOpenHashMap<Value> columnValues = valuesByColumn.get(offset);

                if (columnValues == null) {
                    logger.debug("Column with offset {} has no values, skipping entirely", offset);
                    continue;
                }

                Value.Type columnType = this.columnTypes[offset];
                assert(columnValues != null);

                long[] timeoffsets_ = this.timeoffsets.toArray(new long[this.timeoffsets.size()]);

                switch (columnType) {
                case DOUBLE:
                    qdb.ts_batch_set_pinned_doubles(handle,
                                                    batchTable,
                                                    shard,
                                                    offset,
                                                    timeoffsets_,
                                                    Values.asPrimitiveDoubleArray(columnValues,
                                                                                  matrixHeight));
                    break;

                case INT64:
                    qdb.ts_batch_set_pinned_int64s(handle,
                                                   batchTable,
                                                   shard,
                                                   offset,
                                                   timeoffsets_,
                                                   Values.asPrimitiveInt64Array(columnValues,
                                                                                matrixHeight));
                    break;

                case TIMESTAMP:
                    qdb.ts_batch_set_pinned_timestamps(handle,
                                                       batchTable,
                                                       shard,
                                                       offset,
                                                       timeoffsets_,
                                                       Values.asPrimitiveTimestampArray(columnValues,
                                                                                        matrixHeight));
                    break;

                case BLOB:
                    qdb.ts_batch_set_pinned_blobs(handle,
                                                  batchTable,
                                                  shard,
                                                  offset,
                                                  timeoffsets_,
                                                  Values.asPrimitiveBlobArray(columnValues,
                                                                              matrixHeight));
                    break;

                case STRING:
                    qdb.ts_batch_set_pinned_strings(handle,
                                                    batchTable,
                                                    shard,
                                                    offset,
                                                    timeoffsets_,
                                                    Values.asPrimitiveStringArray(columnValues,
                                                                                  matrixHeight));
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

        this.columnShardSizes = PinnedWriter.tablesToColumnShardSizes(tables);
        this.columnTypes = PinnedWriter.tablesToColumnTypes(tables);
    }

    protected PinnedWriter(Session session, Table[] tables, Writer.PushMode mode) {
        super(session, tables, mode);
        this.currentRow = 0;
        this.matrixByShard = new Long2ObjectOpenHashMap<PinnedMatrix>();

        this.columnShardSizes = PinnedWriter.tablesToColumnShardSizes(tables);
        this.columnTypes = PinnedWriter.tablesToColumnTypes(tables);
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

    @Override
    public void extraTables(Table[] tables) {
        super.extraTables(tables);

        long[] newShardSizes = PinnedWriter.tablesToColumnShardSizes(tables);
        Value.Type[] newColumnTypes = PinnedWriter.tablesToColumnTypes(tables);

        this.columnShardSizes = LongStream
            .concat(Arrays.stream(this.columnShardSizes),
                    Arrays.stream(newShardSizes))
            .toArray();


        this.columnTypes = Stream
            .concat(Arrays.stream(this.columnTypes),
                    Arrays.stream(newColumnTypes))
            .toArray(Value.Type[]::new);

        for (long shard : this.matrixByShard.keySet()) {
            this.matrixByShard
                .get(shard)
                .extraColumns(newColumnTypes);
        }
    }

    @Override
    public void append(Integer offset, Timespec timestamp, Value[] values) throws IOException {
        super.trackMinMaxTimestamp(timestamp);
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

        logger.info("Flushing pinned batch writer");

        Instant startPinTime = Instant.now();

        for (Long shard : this.matrixByShard.keySet()) {
            PinnedMatrix xs = this.matrixByShard.get(shard);
            assert(xs != null);

            xs.flush(this.session.handle(),
                     this.batchTable,
                     shard);
        }

        Instant endPinTime = Instant.now();

        logger.debug("Columns pinned in {}", Duration.between(startPinTime, endPinTime));


        Instant startFlushTime = Instant.now();

        super.flush();

        Instant endFlushTime = Instant.now();

        logger.debug("Columns flushed in {}", Duration.between(startFlushTime, endFlushTime));
    }

    private static long calculateOffset(long shard, Timespec ts) {
        return ts.minusSeconds(shard).toEpochNanos();
    }

    private static long truncateTimespecToShard(long shardSize, long secs) {
        return secs - (secs % shardSize);
    }
}
