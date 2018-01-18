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

    public Timespec getEnd() {
        return this.end;
    }

    public String toString() {
        return "TimeRange (begin: " + this.begin.toString() + ", end: " + this.end.toString() + ")";
    }
}
