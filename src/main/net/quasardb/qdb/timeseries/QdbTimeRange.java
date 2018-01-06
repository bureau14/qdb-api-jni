package net.quasardb.qdb;

import java.io.Serializable;
import net.quasardb.qdb.jni.*;

public class QdbTimeRange implements Serializable {

    protected QdbTimespec begin;
    protected QdbTimespec end;

    public QdbTimeRange (QdbTimespec begin, QdbTimespec end) {
        this.begin = begin;
        this.end = end;
    }

    public QdbTimespec getBegin() {
        return this.begin;
    }

    public QdbTimespec getEnd() {
        return this.end;
    }
    public static qdb_ts_filtered_range toNative(QdbTimeRange input) {
        // :TODO: implement filters, we're always assuming 'no filter' here

        return new qdb_ts_filtered_range(new qdb_ts_range(input.begin.getValue(), input.end.getValue()),
                                         new qdb_ts_no_filter());
    }

    public static QdbTimeRange fromNative(qdb_ts_filtered_range input) {
        // :TODO: implement filters, we're always assuming 'no filter' here
        assert (input.getFilter().getType() == 0);

        return new QdbTimeRange(new QdbTimespec(input.getRange().getBegin()),
                                new QdbTimespec(input.getRange().getEnd()));
    }

    public String toString() {
        return "QdbTimeRange (begin: " + this.begin.toString() + ", end: " + this.end.toString() + ")";
    }
}
