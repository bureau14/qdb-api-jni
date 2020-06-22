package net.quasardb.qdb.ts;

import java.nio.ByteBuffer;
import java.util.List;

import net.quasardb.qdb.jni.Constants;

/**
 * Utility functions that operate on (arrays of) values.
 */
public class Values {

    private static double asDouble(Value in) {
        if (in == null) {
            return Constants.nullDouble;
        }

        switch (in.getType()) {
        case DOUBLE:
            return in.doubleValue;
        case UNINITIALIZED:
            return Constants.nullDouble;
        }

        throw new RuntimeException("Not a double value: " + in.toString());
    }

    private static long asInt64(Value in) {
        if (in == null) {
            return Constants.nullInt64;
        }

        switch (in.getType()) {
        case INT64:
            return in.int64Value;
        case UNINITIALIZED:
            return Constants.nullInt64;
        }

        throw new RuntimeException("Not an int64 value: " + in.toString());
    }

    private static ByteBuffer asBlob(Value in) {
        if (in == null) {
            return Constants.nullBlob;
        }

        switch (in.getType()) {
        case BLOB:
            assert (in.blobValue.isDirect());
            return in.blobValue;
        case UNINITIALIZED:
            return Constants.nullBlob;
        }

        throw new RuntimeException("Not a blob value: " + in.toString());
    }

    private static ByteBuffer asString(Value in) {
        if (in == null) {
            return Constants.nullBlob;
        }

        switch (in.getType()) {
        case STRING:
            assert (in.blobValue != null);
            assert (in.blobValue.isDirect());
            return in.blobValue;
        case UNINITIALIZED:
            return Constants.nullBlob;
        }

        throw new RuntimeException("Not a string value: " + in.toString());
    }

    public static double[] asPrimitiveDoubleArray (Value[] in) {
        double[] out = new double[in.length];
        for (int i = 0; i < in.length; ++i) {
            out[i] = asDouble(in[i]);
        }

        return out;
    }

    public static long[] asPrimitiveInt64Array (Value[] in) {
        long[] out = new long[in.length];
        for (int i = 0; i < in.length; ++i) {
            out[i] = asInt64(in[i]);
        }

        return out;
    }

    public static Timespecs asPrimitiveTimestampArray (Value[] in) {
        long[] sec = new long[in.length];
        long[] nsec = new long[in.length];
        for (int i = 0; i < in.length; ++i) {
            Value v = in[i];

            if (v == null || v.getType () == Value.Type.UNINITIALIZED) {
                sec[i] = Constants.nullTime;
                nsec[i] = Constants.nullTime;
            } else if (v.getType () == Value.Type.TIMESTAMP) {
                sec[i] = v.timestampValue.getSec();
                nsec[i] = v.timestampValue.getNano();
            } else {
                throw new RuntimeException("Not a timestamp value: " + v.toString());
            }
        }

        return new Timespecs(sec, nsec);
    }

    public static ByteBuffer[] asPrimitiveBlobArray (Value[] in) {
        ByteBuffer[] out = new ByteBuffer[in.length];
        for (int i = 0; i < in.length; ++i) {
            out[i] = asBlob(in[i]);
        }

        return out;
    }

    public static ByteBuffer[] asPrimitiveStringArray (Value[] in) {
        ByteBuffer[] out = new ByteBuffer[in.length];
        for (int i = 0; i < in.length; ++i) {
            out[i] = asString(in[i]);
        }

        return out;
    }

};
