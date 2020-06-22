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
    private static final int INITIAL_CHUNK_SIZE = 65536;

    private int currentRow;
    private Long2ObjectOpenHashMap<PinnedMatrix> matrixByShard;
    private Value.Type[] columnTypes;
    private long[] columnShardSizes;

    private Value[][] valuesByColumn;

    public static class PinnedMatrix {
        Value.Type[] columnTypes;
        long[] timeoffsets;
        ArrayList<Value[]> valuesByColumn;
        int currentRow;

        public PinnedMatrix(Value.Type[] columnTypes) {

            this.columnTypes = columnTypes;
            this.currentRow = 0;
            this.timeoffsets = new long[INITIAL_CHUNK_SIZE];

            this.valuesByColumn = PinnedMatrix.toValuesByColumn(columnTypes,  INITIAL_CHUNK_SIZE);
            assert(this.valuesByColumn.size() == columnTypes.length);
        }

        private static ArrayList<Value[]> toValuesByColumn(Value.Type[] columnTypes, int chunkSize) {
            int columnCount = columnTypes.length;
            ArrayList<Value[]> result = new ArrayList<Value[]>(columnCount);

            for (int i = 0; i < columnCount; ++i) {
                result.add(i, new Value[chunkSize]);
            }

            return result;
        }

        void extraColumns(Value.Type[] columnTypes) {
            assert(this.valuesByColumn != null);

            this.valuesByColumn.addAll(PinnedMatrix.toValuesByColumn(columnTypes, INITIAL_CHUNK_SIZE));

            this.columnTypes = Stream
                .concat(Arrays.stream(this.columnTypes),
                        Arrays.stream(columnTypes))
                .toArray(Value.Type[]::new);

        }

        void add (int offset, long timeOffset, Value[] values) {
            this.timeoffsets[this.currentRow] = timeOffset;
            for (int i = 0; i < values.length; ++i) {
                Value[] columnValues = this.valuesByColumn.get(offset + i);
                assert(columnValues.length > this.currentRow);
                //assert(columnValues.size () == this.currentRow);

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

                columnValues[this.currentRow] = values[i];
            }

            ++this.currentRow;

        }

        void flush(long handle, long batchTable, long shard) {
            // `offset` is the relative offset of the column within the total
            // batch writer state.
            assert(this.columnTypes.length == this.valuesByColumn.size());

            for (int offset = 0; offset < this.valuesByColumn.size(); ++offset) {
                Value[] columnValues = valuesByColumn.get(offset);

                if (columnValues.length == 0) {
                    logger.debug("Column with offset {} has no values, skipping entirely", offset);
                    continue;
                }

                // We always expect a 'perfect' matrix
                assert(this.timeoffsets.length == columnValues.length);

                Value.Type columnType = this.columnTypes[offset];
                assert(columnValues != null);

                System.out.println("Pinning column " + Integer.toString(offset) + " with " + Integer.toString(columnValues.length) + " values");

                switch (columnType) {
                case DOUBLE:
                    qdb.ts_batch_set_pinned_doubles(handle,
                                                    batchTable,
                                                    shard,
                                                    offset,
                                                    this.timeoffsets,
                                                    Values.asPrimitiveDoubleArray(columnValues));
                    break;

                case INT64:
                    qdb.ts_batch_set_pinned_int64s(handle,
                                                   batchTable,
                                                   shard,
                                                   offset,
                                                   this.timeoffsets,
                                                   Values.asPrimitiveInt64Array(columnValues));
                    break;

                case TIMESTAMP:
                    qdb.ts_batch_set_pinned_timestamps(handle,
                                                       batchTable,
                                                       shard,
                                                       offset,
                                                       this.timeoffsets,
                                                       Values.asPrimitiveTimestampArray(columnValues));
                    break;

                case BLOB:
                    qdb.ts_batch_set_pinned_blobs(handle,
                                                  batchTable,
                                                  shard,
                                                  offset,
                                                  this.timeoffsets,
                                                  Values.asPrimitiveBlobArray(columnValues));
                    break;

                case STRING:
                    qdb.ts_batch_set_pinned_strings(handle,
                                                    batchTable,
                                                    shard,
                                                    offset,
                                                    this.timeoffsets,
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

            logger.debug("Pinning columns for shard {}", shard);

            xs.flush(this.session.handle(),
                     this.batchTable,
                     shard);
        }

        logger.debug("All columns pinned, handing over to Writer.flush");

        super.flush();

        logger.debug("Flush successful, releasing pinned memory");

    }

    private static long calculateOffset(long shard, Timespec ts) {
        return ts.minusSeconds(shard).toEpochNanos();
    }

    private static long truncateTimespecToShard(long shardSize, long secs) {
        return secs - (secs % shardSize);
    }
}
