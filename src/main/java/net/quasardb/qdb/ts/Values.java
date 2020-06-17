package net.quasardb.qdb.ts;

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
            return Double.NaN;
        }

        throw new RuntimeException("Not a double value: " + in.toString());
    }


    private static long asInt64(Value in) {
        switch (in.getType()) {
        case INT64:
            return in.int64Value;
        case UNINITIALIZED:
            return -1;
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

};
