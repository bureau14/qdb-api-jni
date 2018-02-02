package net.quasardb.qdb.ts;

import net.quasardb.qdb.jni.*;

/**
 * Represents a time range with additional filters applied.
 *
 * Currently serves as a placeholder class, as no filters are supported yet.
 */
public final class FilteredRange {
    TimeRange range;
    qdb_ts_filter filter;

    /**
     * Wraps a {@link TimeRange} without applying any filters.
     */
    public FilteredRange(TimeRange range){
        this(range, new qdb_ts_no_filter());
    }

    protected FilteredRange(TimeRange range, qdb_ts_filter filter) {
        this.range = range;
        this.filter = filter;
    }

    /**
     * Access to the underlying TimeRange.
     */
    public TimeRange getRange() {
        return this.range;
    }

    protected qdb_ts_filter getFilter() {
        return this.filter;
    }

    public String toString() {
        return "FilteredRange(range: " + this.range.toString() + ", filter: " + this.filter.toString() +  ")";
    }
}
