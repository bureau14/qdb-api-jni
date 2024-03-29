package net.quasardb.qdb.ts;

import java.time.Duration;
import java.time.Instant;

import java.util.Arrays;
import java.util.Collection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.Session;


/**
 * Point-based (or column-oriented) API, which can be used for high-speed
 * reading / writing of single-column timeseries data.
 */
public class Points {
    private static final Logger logger = LoggerFactory.getLogger(Points.class);

    public abstract static class Data {
        public Timespecs timespecs;
        public Object values;

        public Data(Timespecs timespecs, Object values) {
            this.timespecs = timespecs;
            this.values = values;

            assert(this.timespecs != null && this.values != null);
        }

        @Override
        public boolean equals(Object o) {
            try {
                Data o_ = (Data) o;
                return this.timespecs.equals(o_.timespecs) && this.valueEquals(o_.values);
            } catch (ClassCastException e) {
                return false;
            }
        }

        abstract protected boolean valueEquals(Object values);

        public int size() {
            // We rely on this.timespecs.size() to be identical to whatever the
            // values size is.
            return this.timespecs.size();
        }

        public String toString() {
            String ret = "<Points.Data>";
            ret += this.timespecs.toString();

            ret += "<Values>";
            ret += this.values.toString();
            ret += "</Values>";
            ret += "</Points.Data>";
            return ret;
        }
    };

    protected abstract static class TypedData <T> extends Data {
        public TypedData(Timespecs timespecs, T values) {
            super(timespecs, (Object)values);
        }

        protected boolean valueEquals(Object values) {
            return typedValueEquals((T)values);
        }

        abstract protected boolean typedValueEquals(T values);
    };

    protected abstract static class ArrayData <T> extends TypedData<T> {
        public ArrayData(Timespecs timespecs, T values) {
            super(timespecs, values);

            assert(values.getClass().isArray() == true);
        }

        abstract protected boolean typedValueEquals(T values);
    };

    public static class BlobData extends ArrayData<ByteBuffer[]> {
        public BlobData(Timespecs timespecs, ByteBuffer[] values) {
            super(timespecs, values);
        }

        protected boolean typedValueEquals(ByteBuffer[] o) {
            return Arrays.equals((ByteBuffer[])this.values, o);
        }
    }

    public static class StringData extends ArrayData<String[]> {
        public StringData(Timespecs timespecs, String[] values) {
            super(timespecs, values);
        }

        protected boolean typedValueEquals(String[] o) {
            return Arrays.equals((String[])this.values, o);
        }
    }

    public static class DoubleData extends ArrayData<double[]> {
        public DoubleData(Timespecs timespecs, double[] values) {
            super(timespecs, values);
        }

        protected boolean typedValueEquals(double[] o) {
            logger.debug("comparing double arrays");
            boolean res = Arrays.equals((double[])this.values, o);
            logger.debug("array equals = {}", res);
            return res;
        }

        @Override
        public String toString() {
            String ret = "<Points.DoubleData>";
            ret += this.timespecs.toString();
            double[] values_ = (double[])this.values;

            ret += "<Values>";
            ret += Arrays.toString(values_);
            ret += "</Values>";
            ret += "</Points.DoubleData>";
            return ret;
        }

    }

    public static class Int64Data extends ArrayData<long[]> {
        public Int64Data(Timespecs timespecs, long[] values) {
            super(timespecs, values);
        }

        protected boolean typedValueEquals(long[] o) {
            return Arrays.equals((long[])this.values, o);
        }
    }

    public static class TimestampData extends TypedData<Timespecs> {
        public TimestampData(Timespecs timespecs, Timespecs values) {
            super(timespecs, values);
            assert(timespecs.size() == values.size());
        }

        protected boolean typedValueEquals(Timespecs o) {
            return ((Timespecs)this.values).equals(o);
        }
    }

    private Value.Type valueType;
    private Data values;

    public Points(Value.Type valueType, Data values) {
        this.valueType = valueType;
        this.values = values;
    }

    public static Points ofBlobs(Timespecs xs, ByteBuffer[] ys) {
        return ofBlobs(new BlobData(xs, ys));
    }

    public static Points ofBlobs(BlobData xs) {
        return new Points(Value.Type.BLOB, xs);
    }

    public static Points ofStrings(Timespecs xs, String[] ys) {
        return ofStrings(new StringData(xs, ys));
    }

    public static Points ofStrings(StringData xs) {
        return new Points(Value.Type.STRING, xs);
    }

    public static Points ofDoubles(Timespecs xs, double[] ys) {
        return ofDoubles(new DoubleData(xs, ys));
    }

    public static Points ofDoubles(DoubleData xs) {
        return new Points(Value.Type.DOUBLE, xs);
    }

    public static Points ofInt64s(Timespecs xs, long[] ys) {
        return ofInt64s(new Int64Data(xs, ys));
    }

    public static Points ofInt64s(Int64Data xs) {
        return new Points(Value.Type.INT64, xs);
    }

    public static Points ofTimestamps(Timespecs xs, Timespecs ys) {
        return ofTimestamps(new TimestampData(xs, ys));
    }

    public static Points ofTimestamps(TimestampData xs) {
        return new Points(Value.Type.TIMESTAMP, xs);
    }

    public BlobData  blobs() {
        assert(this.valueType == Value.Type.BLOB);
        return (BlobData)this.values;
    }

    public StringData strings() {
        assert(this.valueType == Value.Type.STRING);
        return (StringData)this.values;
    }

    public DoubleData doubles() {
        assert(this.valueType == Value.Type.DOUBLE);
        return (DoubleData)this.values;
    }

    public Int64Data int64s() {
        assert(this.valueType == Value.Type.INT64);
        return (Int64Data)this.values;
    }

    public TimestampData timestamps() {
        assert(this.valueType == Value.Type.TIMESTAMP);
        return (TimestampData)this.values;
    }

    public static void insert(Session session,
                              Table table,
                              Column column,
                              Points xs) {
        insert(session, table, column.getName(), xs);
    }

    public static void insert(Session session,
                              String tableName,
                              Column column,
                              Points xs) {
        insert(session, tableName, column.getName(), xs);
    }

    public static void insert(Session session,
                              Table table,
                              String columnName,
                              Points xs) {
        assert(table.hasColumnWithName(columnName) == true);
        insert(session, table.getName(), columnName, xs);
    }

    public static void insert(Session session,
                              String tableName,
                              String columnName,
                              Points xs) {
        logger.debug("Inserting timeseries");
        Instant start = Instant.now();

        qdb.ts_point_insert(session.handle(),
                            tableName,
                            columnName,
                            xs.values.timespecs,
                            xs.valueType.asInt(),
                            xs.values.values);

        Instant stop = Instant.now();
        logger.debug("Inserted {} points in {}", xs.values.size(), Duration.between(start, stop));
    }

    public static Points get(Session session,
                             String tableName,
                             Column column) {
        return get(session,
                   tableName,
                   column.getName(),
                   column.getType());
    }

    public static Points get(Session session,
                             String tableName,
                             Column column,
                             TimeRange range) {
        return get(session,
                   tableName,
                   column.getName(),
                   column.getType(),
                   range);
    }

    public static Points get(Session session,
                             String tableName,
                             Column column,
                             TimeRange[] ranges) {
        return get(session,
                   tableName,
                   column.getName(),
                   column.getType(),
                   ranges);
    }

    public static Points get(Session session,
                             String tableName,
                             String columnName,
                             Column.Type columnType) {
        return get(session,
                   tableName,
                   columnName,
                   columnType.asValueType());
    }

    public static Points get(Session session,
                             String tableName,
                             String columnName,
                             Value.Type valueType) {
        return get(session,
                   tableName,
                   columnName,
                   valueType,
                   TimeRange.UNIVERSE_RANGE);
    }
    public static Points get(Session session,
                             String tableName,
                             String columnName,
                             Column.Type columnType,
                             TimeRange range) {
        return get(session,
                   tableName,
                   columnName,
                   columnType.asValueType(),
                   range);
    }

    public static Points get(Session session,
                             String tableName,
                             String columnName,
                             Value.Type valueType,
                             TimeRange range) {
        return get(session,
                   tableName,
                   columnName,
                   valueType,
                   new TimeRange[]{range});
    }

    public static Points get(Session session,
                             String tableName,
                             String columnName,
                             Column.Type columnType,
                             TimeRange[] ranges) {
        return get(session,
                   tableName,
                   columnName,
                   columnType.asValueType(),
                   ranges);
    }

    public static Points get(Session session,
                             String tableName,
                             String columnName,
                             Value.Type valueType,
                             TimeRange[] ranges) {
        Instant start = Instant.now();

        Points.Data data = qdb.ts_point_get_ranges(session.handle(),
                                                   tableName,
                                                   columnName,
                                                   valueType.asInt(),
                                                   ranges);

        Instant stop = Instant.now();
        logger.debug("Retrieved {} points in {}", data.size(), Duration.between(start, stop));

        return new Points(valueType, data);
    }

    @Override public boolean equals(Object o) {
        if (!(o instanceof Points)) {
            return false;
        }

        Points o_ = (Points) o;

        return this.valueType == o_.valueType && this.values.equals(o_.values);
    }

    public String toString() {
        String ret = "<Points valueType=" + this.valueType + ">";
        ret += this.values.toString();
        ret += "</Points>";
        return ret;
    }

}
