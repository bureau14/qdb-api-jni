package net.quasardb.qdb.ts;

import net.quasardb.qdb.jni.Constants;
import java.util.List;

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
        switch (in.getType()) {
        case INT64:
            return in.int64Value;
        case UNINITIALIZED:
            return Constants.nullInt64;
        }

        throw new RuntimeException("Not an int64 value: " + in.toString());
    }

    public static double[] asPrimitiveDoubleArray (Value[] in) {
        double[] out = new double[in.length];
        for (int i = 0; i < in.length; ++i) {
            out[i] = asDouble(in[i]);
        }

        return out;
    }

    public static double[] asPrimitiveDoubleArray (List<Value> in) {
        double[] out = new double[in.size()];
        for (int i = 0; i < in.size(); ++i) {
            out[i] = asDouble(in.get(i));
        }

        return out;
    }

    public static long[] asPrimitiveInt64Array (List<Value> in) {
        long[] out = new long[in.size()];
        for (int i = 0; i < in.size(); ++i) {
            out[i] = asInt64(in.get(i));
        }

        return out;
    }

    public static Timespecs asPrimitiveTimestampArray (List<Value> in) {
        long[] sec = new long[in.size()];
        long[] nsec = new long[in.size()];
        for (int i = 0; i < in.size(); ++i) {
            Value v = in.get(i);

            switch (v.getType()) {
            case TIMESTAMP:
                sec[i] = v.timestampValue.getSec();
                nsec[i] = v.timestampValue.getNano();
                break;
            case UNINITIALIZED:
                sec[i] = Constants.nullTime;
                nsec[i] = Constants.nullTime;
                break;
            default:
                throw new RuntimeException("Not a timestamp value: " + v.toString());
            }
        }

        return new Timespecs(sec, nsec);
    }

};
