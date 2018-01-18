package net.quasardb.qdb.ts;

import net.quasardb.qdb.jni.*;

public final class FilteredRange {
    TimeRange range;
    qdb_ts_filter filter;

    public FilteredRange(TimeRange range){
        this(range, new qdb_ts_no_filter());
    }

    public FilteredRange(TimeRange range, qdb_ts_filter filter) {
        this.range = range;
        this.filter = filter;
    }

    public TimeRange getRange() {
        return this.range;
    }

    public qdb_ts_filter getFilter() {
        return this.filter;
    }

    public String toString() {
        return "FilteredRange(range: " + this.range.toString() + ", filter: " + this.filter.toString() +  ")";
    }
}
