package net.quasardb.qdb.ts;

import java.util.Arrays;
import java.util.ArrayList;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Efficient, array-based representation of many timespecs. Primarily used for
 * efficient transfer of large amount of timespecs to and from JNI.
 */
public class Timespecs implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(Timespecs.class);

    public long[] sec;
    public long[] nsec;

    public Timespecs(long[] sec, long[] nsec) {
        assert(sec.length == nsec.length);

        this.sec = sec;
        this.nsec = nsec;
    }


    public static Timespecs ofArray(Timespec[] xs) {
        long[] sec  = new long[xs.length];
        long[] nsec = new long[xs.length];

        for (int i = 0; i < xs.length; ++i) {
            sec[i] = xs[i].sec;
            nsec[i] = xs[i].nsec;
        }

        return new Timespecs(sec, nsec);
    }

    public static Timespecs ofArray(ArrayList<Timespec> xs) {
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
        assert(this.sec.length == this.nsec.length);
        return this.sec.length;
    }

    @Override public boolean equals(Object o) {
        if (!(o instanceof Timespecs)) {
            return false;
        }

        Timespecs o_ = (Timespecs) o;

        return Arrays.equals(this.sec, o_.sec) && Arrays.equals(this.nsec, o_.nsec);
    }

    public String toString() {
        StringBuilder ret = new StringBuilder();
        ret.ensureCapacity(32 + (this.sec.length * 64));
        ret.append("<Timespecs>");

        for (int i = 0; i < this.sec.length; ++i) {
            long sec = this.sec[i];
            long nsec = this.nsec[i];
            ret.append("<Timespec sec=").append(sec).append(" nsec=").append(nsec).append(" />");;
        }
        ret.append("</Timespecs>");
        return ret.toString();

    }

};
