package net.quasardb.qdb.ts;

import java.io.Serializable;

import net.quasardb.qdb.jni.*;

public class TimeRange implements Serializable {

    protected Timespec begin;
    protected Timespec end;

    public TimeRange (Timespec begin, Timespec end) {
        this.begin = begin;
        this.end = end;
    }

    public Timespec getBegin() {
        return this.begin;
    }

    /**
     * Returns a copy of this timerange with a different begin.
     */
    public TimeRange withBegin(Timespec b) {
        return new TimeRange(b, this.end);
    }

    public Timespec getEnd() {
        return this.end;
    }

    /**
     * Returns a copy of this timerange with a different end.
     */
    public TimeRange withEnd(Timespec e) {
        return new TimeRange(this.begin, e);
    }

    public String toString() {
        return "TimeRange (begin: " + this.begin.toString() + ", end: " + this.end.toString() + ")";
    }
}
