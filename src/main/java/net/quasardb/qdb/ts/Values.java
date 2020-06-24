package net.quasardb.qdb.ts;

import java.nio.ByteBuffer;
import java.util.*;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import net.quasardb.qdb.jni.Constants;

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

    public static double[] asPrimitiveDoubleArray (Int2ObjectMap<Value> in, int len) {
        double[] out = new double[len];
        Arrays.fill(out, Constants.nullDouble);

        ObjectSet<Int2ObjectMap.Entry<Value>> entries = in.int2ObjectEntrySet();
        for (Int2ObjectMap.Entry<Value> entry : entries) {
            out[entry.getIntKey()] = asDouble((Value)(entry.getValue()));
        }

        return out;
    }

    public static long[] asPrimitiveInt64Array (Int2ObjectMap<Value> in, int len) {
        long[] out = new long[len];
        Arrays.fill(out, Constants.nullInt64);

        ObjectSet<Int2ObjectMap.Entry<Value>> entries = in.int2ObjectEntrySet();
        for (Int2ObjectMap.Entry<Value> entry : entries) {
            out[entry.getIntKey()] = asInt64((Value)(entry.getValue()));
        }

        return out;
    }

    public static Timespecs asPrimitiveTimestampArray (Int2ObjectMap<Value> in, int len) {
        long[] sec = new long[len];
        long[] nsec = new long[len];
        Arrays.fill(sec, Constants.nullTime);
        Arrays.fill(nsec, Constants.nullTime);

        ObjectSet<Int2ObjectMap.Entry<Value>> entries = in.int2ObjectEntrySet();
        for (Int2ObjectMap.Entry<Value> entry : entries) {
            Value v = entry.getValue();
            int i =  entry.getIntKey();

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

    public static ByteBuffer[] asPrimitiveBlobArray (Int2ObjectMap<Value> in, int len) {
        ByteBuffer[] out = new ByteBuffer[len];
        Arrays.fill(out, Constants.nullBlob);

        ObjectSet<Int2ObjectMap.Entry<Value>> entries = in.int2ObjectEntrySet();
        for (Int2ObjectMap.Entry<Value> entry : entries) {
            out[entry.getIntKey()] = asBlob((Value)(entry.getValue()));
        }

        return out;
    }

    public static ByteBuffer[] asPrimitiveStringArray (Int2ObjectMap<Value> in, int len) {
        ByteBuffer[] out = new ByteBuffer[len];
        Arrays.fill(out, Constants.nullBlob);


        ObjectSet<Int2ObjectMap.Entry<Value>> entries = in.int2ObjectEntrySet();
        for (Int2ObjectMap.Entry<Value> entry : entries) {
            out[entry.getIntKey()] = asString((Value)(entry.getValue()));
        }

        return out;
    }

};
