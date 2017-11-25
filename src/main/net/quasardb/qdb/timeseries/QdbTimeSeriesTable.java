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
 * Represents a timeseries table.
 */
public class QdbTimeSeriesTable implements AutoCloseable, Flushable {

    QdbSessionPool pool;
    String name;
    Long localTable;
    Map <String, Integer> columnOffsets;

    /**
     * Initialize a new timeseries table.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries name. Must already exist.
     */
    QdbTimeSeriesTable(QdbSessionPool pool, String name) {
        this.pool = pool;
        this.name = name;

        Reference<qdb_ts_column_info[]> columns =
            new Reference<qdb_ts_column_info[]>();
        int err = qdb.ts_list_columns(this.session.handle(), this.name, columns);
        QdbExceptionFactory.throwIfError(err);

        Reference<Long> theLocalTable = new Reference<Long>();
        qdb.ts_local_table_init(this.session.handle(), this.name, columns.value, theLocalTable);
        QdbExceptionFactory.throwIfError(err);

        this.localTable = theLocalTable.value;

        // Keep track of the columns that are part of this table, so
        // we can later look them up.
        this.columnOffsets = new HashMap(columns.value.length);
        for (int i = 0; i < columns.value.length; ++i) {
            this.columnOffsets.put(columns.value[i].name, i);
        }
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
     * Utility function that looks up a column's index by its id. The first
     * column starts with 0.
     *
     * @param id String identifier of the column.
     * @returns The index of the column inside the timeseries table definition.
     */
    public int columnIndexById (String id) {
        Integer offset = this.columnOffsets.get(id);
        if (offset == null) {
            throw new QdbInvalidArgumentException();
        }

        return offset.intValue();
    }

    /**
     * Append a new row to the local table cache.
     */
    public void append(QdbTimeSeriesRow row) throws IOException {
        int err = qdb.ts_table_row_append(this.localTable, row.getTimestamp().getValue(), row.getValues());
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
