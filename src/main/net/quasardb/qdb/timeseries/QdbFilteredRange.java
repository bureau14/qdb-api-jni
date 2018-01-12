package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

public final class QdbFilteredRange {
    QdbTimeRange range;
    qdb_ts_filter filter;

    public QdbFilteredRange(QdbTimeRange range){
        this(range, new qdb_ts_no_filter());
    }

    public QdbFilteredRange(QdbTimeRange range, qdb_ts_filter filter) {
        this.range = range;
        this.filter = filter;
    }

    public QdbTimeRange getRange() {
        return this.range;
    }

    public qdb_ts_filter getFilter() {
        return this.filter;
    }

    public String toString() {
        return "QdbFilteredRange(range: " + this.range.toString() + ", filter: " + this.filter.toString() +  ")";
    }
}
