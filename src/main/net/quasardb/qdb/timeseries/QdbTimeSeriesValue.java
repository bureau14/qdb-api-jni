package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.jni.*;

/**
 * Represents a timeseries table.
 */
public class QdbTimeSeriesValue {

    protected Type type;
    protected double doubleValue;
    protected ByteBuffer blobValue;

    public enum Type {
        UNINITIALIZED(qdb_ts_column_type.uninitialized),
        DOUBLE(qdb_ts_column_type.double_),
        BLOB(qdb_ts_column_type.blob);

        protected final int value;
        Type(int type) {
            this.value = type;
        }
    }

    /**
     * Represents a double value.
     */
    public static class Double extends QdbTimeSeriesValue {
        public Double(double value) {
            this.type = Type.DOUBLE;
            this.doubleValue = value;
        }
    }

    /**
     * Represents a blob value. Warning: assumes byte array will stay in memory for
     * as long as this object lives.
     */
    public static class Blob extends QdbTimeSeriesValue {
        public Blob(byte[] value) {
            this.type = Type.BLOB;
            this.blobValue = ByteBuffer.wrap(value);
        }

        public Blob(ByteBuffer value) {
            this.type = Type.BLOB;
            this.blobValue = value.duplicate();
        }
    }

    /**
     * Represents a safe blob value that copies the byte array.
     */
    public static class SafeBlob extends QdbTimeSeriesValue {
        public SafeBlob(byte[] value) {
            this.type = Type.BLOB;

            int size = value.length;
            this.blobValue = ByteBuffer.allocateDirect(size);
            this.blobValue.put(value, 0, size);
            this.blobValue.rewind();
        }

        public SafeBlob(ByteBuffer value) {
            this.type = Type.BLOB;

            int size = value.capacity();
            this.blobValue = ByteBuffer.allocateDirect(size);
            this.blobValue.put(value);
            this.blobValue.rewind();
        }
    }

    public Type getType() {
        return this.type;
    }

    public double getDouble() {
        if (this.type != Type.DOUBLE) {
            throw new QdbIncompatibleTypeException();
        }

        return this.doubleValue;
    }

    public ByteBuffer getBlob() {
        if (this.type != Type.BLOB) {
            throw new QdbIncompatibleTypeException();
        }

        return this.blobValue;
    }

}
