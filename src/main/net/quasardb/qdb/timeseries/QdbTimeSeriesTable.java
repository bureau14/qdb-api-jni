package net.quasardb.qdb;

import java.io.IOException;
import java.io.Flushable;
import java.lang.AutoCloseable;
import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a timeseries table.
 */
public final class QdbTimeSeriesTable implements AutoCloseable, Flushable {

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
            qdb.ts_local_table_release(this.session.handle(), this.localTable);
        } finally {
            super.finalize();
        }
    }

    /**
     * Closes the timeseries table and local cache so that memory can be reclaimed. Flushes
     * all remaining output.
     */
    public void close() throws IOException {
        this.flush();
        qdb.ts_local_table_release(this.session.handle(), this.localTable);

        this.localTable = null;
    }

    /**
     * Flush current local cache to server.
     */
    public void flush() throws IOException {
        int err = qdb.ts_push(this.localTable);
        QdbExceptionFactory.throwIfError(err);
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
}
