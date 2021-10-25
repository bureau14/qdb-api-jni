package net.quasardb.qdb.ts;

import java.util.ArrayList;
import java.io.Serializable;

/**
 * Efficient, array-based representation of many timespecs. Primarily used for
 * efficient transfer of large amount of timespecs to and from JNI.
 */
public class Timespecs implements Serializable {
    public long[] sec;
    public long[] nsec;

    public Timespecs(long[] sec, long[] nsec) {
        assert(sec.length == nsec.length);

        this.sec = sec;
        this.nsec = nsec;
    }


    static Timespecs ofArray(Timespec[] xs) {
        long[] sec  = new long[xs.length];
        long[] nsec = new long[xs.length];

        for (int i = 0; i < xs.length; ++i) {
            sec[i] = xs[i].sec;
            nsec[i] = xs[i].nsec;
        }

        return new Timespecs(sec, nsec);
    }

    static Timespecs ofArray(ArrayList<Timespec> xs) {
        long[] sec  = new long[xs.size()];
        long[] nsec = new long[xs.size()];


        int i = 0;
        for (Timespec ts : xs) {
            sec[i]  = ts.sec;
            nsec[i] = ts.nsec;
            ++i;
        }

        return new Timespecs(sec, nsec);
    }

    public int size() {
        return this.sec.length;
    }

};
