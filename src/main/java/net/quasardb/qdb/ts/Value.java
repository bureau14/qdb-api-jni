package net.quasardb.qdb.ts;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.IncompatibleTypeException;

/**
 * Represents a timeseries value.
 */
public class Value implements Serializable, Comparable<Value> {

    Type type;
    long int64Value = Constants.nullInt64;
    double doubleValue = Constants.nullDouble;
    Timespec timestampValue = new Timespec();
    String stringValue = Constants.nullString;
    ByteBuffer blobValue = Constants.nullBlob;

    public enum Type {
        UNINITIALIZED(Constants.qdb_ts_column_uninitialized),
        DOUBLE(Constants.qdb_ts_column_double),
        BLOB(Constants.qdb_ts_column_blob),
        STRING(Constants.qdb_ts_column_string),
        INT64(Constants.qdb_ts_column_int64),
        TIMESTAMP(Constants.qdb_ts_column_timestamp)
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
            case Constants.qdb_ts_column_double:
                return Type.DOUBLE;

            case Constants.qdb_ts_column_blob:
                return Type.BLOB;

            case Constants.qdb_ts_column_string:
                return Type.STRING;

            case Constants.qdb_ts_column_int64:
                return Type.INT64;

            case Constants.qdb_ts_column_timestamp:
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
     * Creates new value out of this value, that is, copies the underlying value.
     */
    public Value(Value value) {
        this.type = value.type;

        switch (this.type) {
        case INT64:
            this.int64Value = value.int64Value;
            break;
        case DOUBLE:
            this.doubleValue = value.doubleValue;
            break;
        case TIMESTAMP:
            this.timestampValue = new Timespec(value.timestampValue);
            break;
        case STRING:
            this.stringValue = new String(value.stringValue);
            break;
        case BLOB:
            this.blobValue = ByteBuffer.allocateDirect(value.blobValue.capacity());
            this.blobValue.put(value.blobValue);
            this.blobValue.rewind();
            value.blobValue.rewind();

            break;
        };
    };


    /**
     * Create a null / empty value.
     */
    public static Value createNull() {
        return new Value(Type.UNINITIALIZED);
    }


    public void setNative(long batchTable, Type columnType, int offset) {

        Type t = (this.type == Type.UNINITIALIZED
                  ? columnType
                  : this.type);



        switch (t) {
        case DOUBLE:
            qdb.ts_batch_row_set_double(batchTable, offset, this.doubleValue);
            break;
        case INT64:
            qdb.ts_batch_row_set_int64(batchTable, offset, this.int64Value);
            break;
        case TIMESTAMP:
            qdb.ts_batch_row_set_timestamp(batchTable, offset,
                                           this.timestampValue.sec,
                                           this.timestampValue.nsec);
            break;
        case BLOB:
            qdb.ts_batch_row_set_blob(batchTable, offset, this.blobValue);
            break;
        case STRING:
            // Convert string to ByteBuffer before passing over to JNI so that
            // we can keep the JNI code really simple (=> fast).
            qdb.ts_batch_row_set_string(batchTable, offset,
                                        (this.stringValue == Constants.nullString
                                         ? null
                                         : this.stringValue.getBytes(StandardCharsets.UTF_8)));
            break;

        case UNINITIALIZED:
            throw new RuntimeException("Setting null");
        }
    }

    /**
     * Updates value to represent an unintialised value.
     */
    public void setNull() {
        this.type = Type.UNINITIALIZED;
    }

    /**
     * Returns true if this value is null.
     */
    public boolean isNull() {
        return this.type == Type.UNINITIALIZED;
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
     * Updates value to take a certain blob value. Will create a copy of the byte array.
     */
    public void setSafeBlob(byte[] value) {
        this.type = Type.BLOB;

        int size = value.length;
        this.blobValue = ByteBuffer.allocateDirect(size);
        this.blobValue.put(value, 0, size);
        this.blobValue.rewind();
    }

    /**
     * Updates value to take a certain blob value. Will create copy of the byte array.
     */
    public void setSafeBlob(ByteBuffer value) {
        this.type = Type.BLOB;

        int size = value.capacity();
        this.blobValue = ByteBuffer.allocateDirect(size);
        this.blobValue.put(value);
        this.blobValue.rewind();
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
     * Update this value to be a String.
     */
    public void setString(String value) {
        this.type = Type.STRING;
        this.stringValue = value;
    }

    /**
     * Create a new String value.
     *
     * @param value String representation of value.
     */
    public static Value createString(String value) {
        Value val = new Value(Type.STRING);
        val.stringValue = value;
        return val;

    }

    /**
     * If this Value's type is a string, ensures that it creates a
     * directly allocated ByteBuffer with a copy of the UTF-8 string representation.
     *
     * Because this implies that the String is then represented as a direct ByteBuffer
     * with a 'stable' memory region behind it, it'll allow us to use this buffer
     * in native code safely for extended periods of time without requiring a copy.
     *
     * @return Returns this value.
     */
    public Value ensureByteBufferBackedString() {
        // TODO(leon): once pinned writers have stabilized, we should
        // always represent all strings as bytebuffers immeidately.
        assert(this.type == Type.STRING);
        assert(this.stringValue != null);

        if (this.blobValue == null) {
            byte[] bs = this.stringValue.getBytes(StandardCharsets.UTF_8);
            int size = bs.length;
            this.blobValue = ByteBuffer.allocateDirect(size);
            this.blobValue.put(bs, 0, size);
            this.blobValue.rewind();
        }

        assert(this.blobValue != null);
        return this;

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

        case STRING:
            return this.getString().equals(rhs.getString());

        case UNINITIALIZED:
            // null == null always true
            return true;
        }

        return false;
    }

    @Override
    public int compareTo(Value rhs) {
        if (this.getType().asInt() < rhs.getType().asInt()) {
            return -1;
        } else if (this.getType().asInt() > rhs.getType().asInt()) {
            return 1;
        };

        switch (this.getType()) {
        case INT64:
            if (this.getInt64() < rhs.getInt64()) {
                return -1;
            } else if (this.getInt64() > rhs.getInt64()) {
                return 1;
            };
            return 0;

        case DOUBLE:
            if (this.getDouble() < rhs.getDouble()) {
                return -1;
            } else if (this.getDouble() > rhs.getDouble()) {
                return 1;
            };
            return 0;

        case TIMESTAMP:
            return this.getTimestamp().compareTo(rhs.getTimestamp());

        case STRING:
            return this.getString().compareTo(rhs.getString());

        case BLOB:
            return this.getBlob().compareTo(rhs.getBlob());

        default:
            break;
        };

        throw new RuntimeException("Unrecognized value type: " + this.getType().toString());
    };

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

        case STRING:
            stream.writeObject(this.stringValue);
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

        case STRING:
            this.stringValue = (String)(stream.readObject());
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
            throw new IncompatibleTypeException("Not an integer: " + this.type.toString());
        }

        return this.int64Value;
    }

    public double getDouble() {
        if (this.type != Type.DOUBLE) {
            throw new IncompatibleTypeException("Not a double: " + this.type.toString());
        }

        return this.doubleValue;
    }

    public Timespec getTimestamp() {
        if (this.type != Type.TIMESTAMP) {
            throw new IncompatibleTypeException("Not a timestamp: " + this.type.toString());
        }

        return this.timestampValue;
    }

    public ByteBuffer getBlob() {
        if (this.type != Type.BLOB) {
            throw new IncompatibleTypeException("Not a blob: " + this.type.toString());
        }

        return this.blobValue.asReadOnlyBuffer();
    }

    public String getString() {
        if (this.type != Type.STRING) {
            throw new IncompatibleTypeException("Not a string: " + this.type.toString());
        }

        return this.stringValue;
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

        case STRING:
            return "Value (type = STRING, value = '" + this.stringValue + "')";

        case UNINITIALIZED:
            return "Value (type = NULL)";
        }

        return "Value (type = INVALID)";
    }

}
