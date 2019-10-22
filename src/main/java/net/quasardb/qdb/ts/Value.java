package net.quasardb.qdb.ts;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.ExceptionFactory;
import net.quasardb.qdb.exception.IncompatibleTypeException;

/**
 * Represents a timeseries table.
 */
public class Value implements Serializable {

    Type type;
    long int64Value;
    double doubleValue;
    Timespec timestampValue;
    ByteBuffer blobValue;

    public enum Type {
        UNINITIALIZED(qdb_ts_column_type.uninitialized),
        DOUBLE(qdb_ts_column_type.double_),
        BLOB(qdb_ts_column_type.blob),
        INT64(qdb_ts_column_type.int64),
        TIMESTAMP(qdb_ts_column_type.timestamp)
        ;

        protected final int value;
        Type(int type) {
            this.value = type;
        }

        public int asInt() {
            return this.value;
        }

        public static Type fromInt(int type) {
            switch(type) {
            case qdb_ts_column_type.double_:
                return Type.DOUBLE;

            case qdb_ts_column_type.blob:
                return Type.BLOB;

            case qdb_ts_column_type.int64:
                return Type.INT64;

            case qdb_ts_column_type.timestamp:
                return Type.TIMESTAMP;
            }

            return Type.UNINITIALIZED;
        }
    }

    public Value() {
        this(Type.UNINITIALIZED);
    }

    protected Value(Type type) {
        this.type = type;
    }

    /**
     * Create a null / empty value.
     */
    public static Value createNull() {
        return new Value(Type.UNINITIALIZED);
    }

    /**
     * Updates value to represent an unintialised value.
     */
    public void setNull() {
        this.type = Type.UNINITIALIZED;
    }

    /**
     * Represents a long integer
     */
    public static Value createInt64(long value) {
        Value val = new Value(Type.INT64);
        val.int64Value = value;
        return val;
    }

    /**
     * Updates value to take a certain long integer value;
     */
    public void setInt64(long value) {
        this.type = Type.INT64;
        this.int64Value = value;
    }

    /**
     * Represents a double value.
     */
    public static Value createDouble(double value) {
        Value val = new Value(Type.DOUBLE);
        val.doubleValue = value;
        return val;
    }

    /**
     * Updates value to take a certain double value;
     */
    public void setDouble(double value) {
        this.type = Type.DOUBLE;
        this.doubleValue = value;
    }

    /**
     * Represents a timestamp
     */
    public static Value createTimestamp(Timespec value) {
        Value val = new Value(Type.TIMESTAMP);
        val.timestampValue = value;
        return val;
    }

    /**
     * Updates value to take a certain timestamp;
     */
    public void setTimestamp(Timespec value) {
        this.type = Type.TIMESTAMP;
        this.timestampValue = value;
    }

    /**
     * Represents a blob value. Warning: assumes byte array will stay in memory for
     * as long as this object lives.
     */
    public static Value createBlob(byte[] value) {
        Value val = new Value(Type.BLOB);
        val.blobValue = ByteBuffer.wrap(value);
        return val;
    }

    /**
     * Represents blob value. Warning: assumes bytebuffer will stay in memory for as
     * long as this object lives.
     */
    public static Value createBlob(ByteBuffer value) {
        Value val = new Value(Type.BLOB);
        val.blobValue = value.duplicate();
        return val;
    }

    /**
     * Updates value to take a certain blob value. Warning: assumes byte array will
     * stay in memory for as long as this object lives.
     */
    public void setBlob(byte[] value) {
        this.type = Type.BLOB;
        this.blobValue = ByteBuffer.wrap(value);
    }

    /**
     * Updates value to take a certain blob value;
     */
    public void setBlob(ByteBuffer value) {
        this.type = Type.BLOB;
        this.blobValue = value.duplicate();
    }

    /**
     * Represents a safe blob value that copies the byte array.
     */
    public static Value createSafeBlob(byte[] value) {
        Value val = new Value(Type.BLOB);

        int size = value.length;
        val.blobValue = ByteBuffer.allocateDirect(size);
        val.blobValue.put(value, 0, size);
        val.blobValue.rewind();

        return val;
    }

    /**
     * Creates a copy of a ByteBuffer into this Value.
     */
    public static Value createSafeBlob(ByteBuffer value) {
        Value val = new Value(Type.BLOB);

        int size = value.capacity();
        val.blobValue = ByteBuffer.allocateDirect(size);
        val.blobValue.put(value);
        val.blobValue.rewind();

        value.rewind();

        return val;
    }

    /**
     * Convenience function that coerces a String to a blob value. Creates
     * copy of string. Assumes default character encoding type.
     *
     * @param value String representation of value.
     */
    public static Value createSafeString(String value) {
        return createSafeBlob(value.getBytes());
    }

    /**
     * Convenience function that coerces a String to a blob value. Creates
     * copy of string, and interprets bytes using specific charset.
     *
     * @param value String representation of value.
     * @param charset Character set to map string characters to bytes.
     */
    public static Value createSafeString(String value, Charset charset) {
        return createSafeBlob(value.getBytes(charset));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Value)) return false;
        Value rhs = (Value)obj;

        if (this.getType() != rhs.getType()) {
            return false;
        }

        switch (this.getType()) {
        case INT64:
            return this.getInt64() == rhs.getInt64();

        case DOUBLE:
            return this.getDouble() == rhs.getDouble();

        case TIMESTAMP:
            return this.getTimestamp().equals(rhs.getTimestamp());

        case BLOB:
            return this.getBlob().equals(rhs.getBlob());

        case UNINITIALIZED:
            // null == null always true
            return true;
        }

        return false;
    }

    private void writeObject(java.io.ObjectOutputStream stream)
        throws IOException {
        stream.writeInt(this.type.value);

        switch (this.type) {
        case INT64:
            stream.writeLong(this.int64Value);
            break;

        case DOUBLE:
            stream.writeDouble(this.doubleValue);
            break;

        case TIMESTAMP:
            stream.writeObject(this.timestampValue);
            break;

        case BLOB:
            writeBlobValue(stream, this.blobValue);
            break;
        }
    }

    private static void writeBlobValue(java.io.ObjectOutputStream stream, ByteBuffer value)
        throws IOException {
        int size = value.capacity();

        byte[] buffer = new byte[size];
        value.get(buffer, 0, size);

        stream.writeInt(value.capacity());
        stream.write(buffer, 0, size);

        value.rewind();
    }


    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        this.type = Type.fromInt(stream.readInt());

        switch (this.type) {
        case INT64:
            this.int64Value = stream.readLong();
            break;

        case DOUBLE:
            this.doubleValue = stream.readDouble();
            break;

        case TIMESTAMP:
            this.timestampValue = (Timespec)(stream.readObject());
            break;

        case BLOB:
            this.blobValue = readBlobValue(stream);
            break;
        }
    }

    private ByteBuffer readBlobValue(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        int size = stream.readInt();

        byte[] buffer = new byte[size];
        stream.read(buffer, 0, size);

        ByteBuffer bb = ByteBuffer.allocateDirect(size);
        bb.put(buffer, 0, size);
        bb.rewind();

        return bb.duplicate();
    }

    public Type getType() {
        return this.type;
    }

    public long getInt64() {
        if (this.type != Type.INT64) {
            throw new IncompatibleTypeException();
        }

        return this.int64Value;
    }

    public double getDouble() {
        if (this.type != Type.DOUBLE) {
            throw new IncompatibleTypeException();
        }

        return this.doubleValue;
    }

    public Timespec getTimestamp() {
        if (this.type != Type.TIMESTAMP) {
            throw new IncompatibleTypeException();
        }

        return this.timestampValue;
    }

    public ByteBuffer getBlob() {
        if (this.type != Type.BLOB) {
            throw new IncompatibleTypeException();
        }

        return this.blobValue.asReadOnlyBuffer();
    }

    public String toString() {

        switch (this.type) {
        case INT64:
            return "Value (type = INT64, value = " + this.int64Value + ")";

        case DOUBLE:
            return "Value (type = DOUBLE, value = " + this.doubleValue + ")";

        case TIMESTAMP:
            return "Value (type = TIMESTAMP, value = " + this.timestampValue + ")";

        case BLOB:
            return "Value (type = BLOB, value = " + this.blobValue.hashCode() + ")";

        case UNINITIALIZED:
            return "Value (type = NULL)";
        }

        return "Value (type = INVALID)";
    }

}
