package net.quasardb.qdb.ts;

import java.io.*;
import java.nio.ByteBuffer;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;

/**
 * Represents a timeseries table.
 */
public class Value implements Serializable {

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    public Type type;
    public long int64Value;
    public double doubleValue;
    public Timespec timestampValue;
    public ByteBuffer blobValue;

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
     * Represents a long integer
     */
    public static Value createTimestamp(Timespec value) {
        Value val = new Value(Type.TIMESTAMP);
        val.timestampValue = value;
        return val;
    }

    /**
     * Updates value to take a certain long integer value;
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
        System.out.println("setBlob, value.hashCode() = " + value.hashCode());
        byte[] buf = new byte[value.capacity()];
        value.get(buf);
        value.rewind();
        System.out.println("setBlob, value hex = " + bytesToHex(buf));

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

    public static Value createSafeBlob(ByteBuffer value) {
        System.out.println("createSafeBlob from ByteBuffer, value.hashCode() = " + value.hashCode());
        byte[] buf = new byte[value.capacity()];
        value.get(buf);
        value.rewind();
        System.out.println("createSafeBlob from ByteBuffer, value hex = " + bytesToHex(buf));

        Value val = new Value(Type.BLOB);

        int size = value.capacity();
        val.blobValue = ByteBuffer.allocateDirect(size);
        val.blobValue.put(value);
        val.blobValue.rewind();

        value.rewind();

        return val;
    }

    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
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
            throw new QdbIncompatibleTypeException();
        }

        return this.int64Value;
    }

    public double getDouble() {
        if (this.type != Type.DOUBLE) {
            throw new QdbIncompatibleTypeException();
        }

        return this.doubleValue;
    }

    public Timespec getTimestamp() {
        if (this.type != Type.TIMESTAMP) {
            throw new QdbIncompatibleTypeException();
        }

        return this.timestampValue;
    }

    public ByteBuffer getBlob() {
        if (this.type != Type.BLOB) {
            throw new QdbIncompatibleTypeException();
        }

        System.out.println("getBlob, value.hashCode() = " + this.blobValue.hashCode());
        byte[] buf = new byte[this.blobValue.capacity()];
        this.blobValue.get(buf);
        this.blobValue.rewind();
        System.out.println("getBlob, value hex = " + bytesToHex(buf));


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
        }

        return "Value (type = INVALID)";
    }

}
