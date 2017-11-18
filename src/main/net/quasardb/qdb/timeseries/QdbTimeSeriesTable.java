package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a timeseries table.
 */
public final class QdbTimeSeriesTable {

    QdbSession session;
    String name;
    Long localTable;

    /**
     * Initialize a new timeseries table.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries name. Must already exist.
     */
    QdbTimeSeriesTable(QdbSession session, String name) {
        this.session = session;
        this.name = name;

        Reference<qdb_ts_column_info[]> columns =
            new Reference<qdb_ts_column_info[]>();
        int err = qdb.ts_list_columns(this.session.handle(), this.name, columns);
        QdbExceptionFactory.throwIfError(err);

        Reference<Long> theLocalTable = new Reference<Long>();
        qdb.ts_local_table_init(this.session.handle(), this.name, columns.value, theLocalTable);
        QdbExceptionFactory.throwIfError(err);

        this.localTable = theLocalTable.value;
    }

    /**
     * Cleans up the internal representation of the local table.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            qdb.ts_local_table_release(this.session.handle(), localTable);
        } finally {
            super.finalize();
        }
    }

    /**
     * Returns the timeseries table name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Append a new row to the local table cache.
     */
    public void append(QdbTimeSeriesRow row) {
        int err = qdb.ts_table_row_append(this.localTable, row.getTimestamp().getValue(), row.getValues());
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Flush current local cache to server.
     */
    public void flush() {
        int err = qdb.ts_push(this.localTable);
        QdbExceptionFactory.throwIfError(err);
    }
}
