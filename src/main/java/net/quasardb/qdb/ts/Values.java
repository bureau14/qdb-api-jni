package net.quasardb.qdb.ts;

import java.nio.ByteBuffer;
import java.util.*;

import net.quasardb.qdb.jni.Constants;
import net.quasardb.qdb.jni.qdb;

/**
 * Utility functions that operate on (arrays of) values.
 */
public class Values {

    private static double asDouble(Value in) {

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

    public static double[] asPrimitiveDoubleArray (ArrayList<Value> in) {
        int len = in.size();
        double[] out = new double[len];

        for (int i = 0; i < len; ++i) {
            out[i] = asDouble(in.get(i));
        }

        return out;
    }

    public static long[] asPrimitiveInt64Array  (ArrayList<Value> in) {
        int len = in.size();
        long[] out = new long[len];

        for (int i = 0; i < len; ++i) {
            out[i] = asInt64(in.get(i));
        }

        return out;
    }

    public static Timespecs asPrimitiveTimestampArray (ArrayList<Value> in) {
        int len = in.size();
        long[] sec = new long[len];
        long[] nsec = new long[len];
        Arrays.fill(sec, Constants.nullTime);
        Arrays.fill(nsec, Constants.nullTime);

        for (int i = 0; i < len; ++i) {
            Value v = in.get(i);

            if (v.getType () == Value.Type.UNINITIALIZED) {
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

    public static ByteBuffer[] asPrimitiveBlobArray (ArrayList<Value> in) {
        int len = in.size();
        ByteBuffer[] out = new ByteBuffer[len];

        for (int i = 0; i < len; ++i) {
            out[i] = asBlob(in.get(i));
        }

        return out;
    }

    public static ByteBuffer[] asPrimitiveStringArray (ArrayList<Value> in) {
        int len = in.size();
        ByteBuffer[] out = new ByteBuffer[len];

        for (int i = 0; i < len; ++i) {
            out[i] = asString(in.get(i));
        }

        return out;
    }
};
