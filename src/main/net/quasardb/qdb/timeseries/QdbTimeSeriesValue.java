package net.quasardb.qdb;

import java.io.*;
import java.nio.ByteBuffer;
import net.quasardb.qdb.jni.*;

/**
 * Represents a timeseries table.
 */
public class QdbTimeSeriesValue implements Serializable {

    public Type type;
    public double doubleValue;
    public ByteBuffer blobValue;

    public enum Type {
        UNINITIALIZED(qdb_ts_column_type.uninitialized),
        DOUBLE(qdb_ts_column_type.double_),
        BLOB(qdb_ts_column_type.blob);

        protected final int value;
        Type(int type) {
            this.value = type;
        }

        protected static Type fromInt(int type) {
            switch(type) {
            case qdb_ts_column_type.double_:
                return Type.DOUBLE;

            case qdb_ts_column_type.blob:
                return Type.BLOB;
            }

            return Type.UNINITIALIZED;
        }
    }

    protected QdbTimeSeriesValue(Type type) {
        this.type = type;
    }

    /**
     * Represents a double value.
     */
    public static QdbTimeSeriesValue createDouble(double value) {
        QdbTimeSeriesValue val = new QdbTimeSeriesValue(Type.DOUBLE);
        val.doubleValue = value;
        return val;
    }

    /**
     * Represents a blob value. Warning: assumes byte array will stay in memory for
     * as long as this object lives.
     */
    public static QdbTimeSeriesValue createBlob(byte[] value) {
        QdbTimeSeriesValue val = new QdbTimeSeriesValue(Type.BLOB);
        val.blobValue = ByteBuffer.wrap(value);
        return val;
    }

    public static QdbTimeSeriesValue createBlob(ByteBuffer value) {
        QdbTimeSeriesValue val = new QdbTimeSeriesValue(Type.BLOB);
        val.blobValue = value.duplicate();
        return val;
    }

    /**
     * Represents a safe blob value that copies the byte array.
     */
    public static QdbTimeSeriesValue createSafeBlob(byte[] value) {
        QdbTimeSeriesValue val = new QdbTimeSeriesValue(Type.BLOB);

        int size = value.length;
        val.blobValue = ByteBuffer.allocateDirect(size);
        val.blobValue.put(value, 0, size);
        val.blobValue.rewind();

        return val;
    }

    public static QdbTimeSeriesValue createSafeBlob(ByteBuffer value) {
        QdbTimeSeriesValue val = new QdbTimeSeriesValue(Type.BLOB);

        int size = value.capacity();
        val.blobValue = ByteBuffer.allocateDirect(size);
        val.blobValue.put(value);
        val.blobValue.rewind();

        value.rewind();

        return val;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbTimeSeriesValue)) return false;
        QdbTimeSeriesValue rhs = (QdbTimeSeriesValue)obj;

        if (this.getType() != rhs.getType()) {
            return false;
        }

        switch (this.getType()) {
        case DOUBLE:
            return this.getDouble() == rhs.getDouble();

        case BLOB:
            return this.getBlob().equals(rhs.getBlob());
        }

        return false;
    }

    private void writeObject(java.io.ObjectOutputStream stream)
        throws IOException {
        stream.writeInt(this.type.value);

        switch (this.type) {
        case DOUBLE:
            stream.writeDouble(this.doubleValue);
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
        case DOUBLE:
            this.doubleValue = stream.readDouble();
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
