package net.quasardb.qdb.ts;

import java.util.List;

/**
 * Utility functions that operate on (arrays of) values.
 */
public class Values {

    public static double[] asPrimitiveDoubleArray (Value[] in) {
        double[] out = new double[in.length];
        for (int i = 0; i < in.length; ++i) {
            assert(in[i].getType () == Value.Type.DOUBLE);
            out[i] = in[i].doubleValue;
        }

        return out;
    }

    public static double[] asPrimitiveDoubleArray (List<Value> in) {
        double[] out = new double[in.size()];
        for (int i = 0; i < in.size(); ++i) {
            assert(in.get(i).getType () == Value.Type.DOUBLE);
            out[i] = in.get(i).doubleValue;
        }

        return out;
    }

};
