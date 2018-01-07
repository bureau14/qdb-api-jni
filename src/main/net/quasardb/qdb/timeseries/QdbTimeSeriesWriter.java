package net.quasardb.qdb;

import java.io.IOException;
import java.io.Flushable;
import java.lang.AutoCloseable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * High-performance bulk writer for a QuasarDB timeseries table.
 */
public class QdbTimeSeriesWriter implements AutoCloseable, Flushable {
    QdbSession session;
    QdbTimeSeriesTable table;
    Long localTable;

    public QdbTimeSeriesWriter(QdbSession session, QdbTimeSeriesTable table) {
        this.session = session;
        this.table = table;

        Reference<Long> theLocalTable = new Reference<Long>();
        int err = qdb.ts_local_table_init(this.session.handle(), table.getName(), table.getColumnInfo(), theLocalTable);
        QdbExceptionFactory.throwIfError(err);

        this.localTable = theLocalTable.value;
    }

    /**
     * Returns the underlying table that is being written to.
     */
    public QdbTimeSeriesTable getTable() {
        return this.table;
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
     * Append a new row to the local table cache.
     */
    public void append(QdbTimeSeriesRow row) throws IOException {
        int err = qdb.ts_table_row_append(this.localTable, row.getTimestamp(), row.getValues());
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Append a new row to the local table cache.
     */
    public void append(QdbTimespec timestamp, QdbTimeSeriesValue[] value) throws IOException {
        this.append(new QdbTimeSeriesRow(timestamp, value));
    }

    /**
     * Append a new row to the local table cache.
     */
    public void append(LocalDateTime timestamp, QdbTimeSeriesValue[] value) throws IOException {
        this.append(new QdbTimeSeriesRow(timestamp, value));
    }

    /**
     * Append a new row to the local table cache.
     */
    public void append(Timestamp timestamp, QdbTimeSeriesValue[] value) throws IOException {
        this.append(new QdbTimeSeriesRow(timestamp, value));
    }
}
