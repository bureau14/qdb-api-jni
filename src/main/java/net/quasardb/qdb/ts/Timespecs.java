package net.quasardb.qdb.ts;

import java.io.Serializable;

/**
 * Efficient, array-based representation of many timespecs. Primarily used for
 * efficient transfer of large amount of timespecs to and from JNI.
 */
public class Timespecs implements Serializable {
    public long[] sec;
    public long[] nsec;

    public Timespecs(long[] sec, long[] nsec) {
        this.sec = sec;
        this.nsec = nsec;
    }

};
