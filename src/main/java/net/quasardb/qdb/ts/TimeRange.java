package net.quasardb.qdb.ts;

import java.io.Serializable;

import net.quasardb.qdb.exception.InvalidArgumentException;
import net.quasardb.qdb.jni.*;

public class TimeRange implements Serializable {

    /**
     * Timerange that spans the universal set of all representable time.
     */
    public static final TimeRange UNIVERSE_RANGE = new TimeRange(Timespec.MIN_VALUE,
                                                                 Timespec.MAX_VALUE);

    protected Timespec begin;
    protected Timespec end;

    public TimeRange (Timespec begin, Timespec end) {
        if (end.isBefore(begin)) {
            throw new InvalidArgumentException("Timerange must satisfy requirement begin <= end, but got begin(" + begin.toString() + ") and end(" + end.toString() + ") instead");
        }

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

    /**
     * Returns the union of two time ranges, that is, the range that can contain both
     * time ranges.
     */
    public static TimeRange union(TimeRange lhs, TimeRange rhs) {
        return new TimeRange(Timespec.min(lhs.begin, rhs.begin),
                             Timespec.max(lhs.end, rhs.end));
    }

    /**
     * Returns the intersection of two time ranges, that is, the widest possible range that
     * is contained by both time ranges.
     *
     * Undefined behavior if the two timespecs do not overlap.
     */
    public static TimeRange intersect(TimeRange lhs, TimeRange rhs) {
        return new TimeRange(Timespec.max(lhs.begin, rhs.begin),
                             Timespec.min(lhs.end, rhs.end));
    }

    /**
     * Merges a new timespec into this time range, and widens the time range if necessary
     * to be wide enough to contain this time point.
     */
    public static TimeRange merge(TimeRange r, Timespec t) {
        r.begin = Timespec.min(r.begin, t);
        r.end = Timespec.max(r.end, t);
        return r;
    }
}
