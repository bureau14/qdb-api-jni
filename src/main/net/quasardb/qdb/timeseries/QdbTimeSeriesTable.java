package net.quasardb.qdb;

import java.io.IOException;
import java.io.Serializable;
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
public class QdbTimeSeriesTable implements Serializable {
    String name;
    qdb_ts_column_info[] columns;
    Map <String, Integer> columnOffsets;

    /**
     * Initialize a new timeseries table.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries name. Must already exist.
     */
    QdbTimeSeriesTable(QdbSession session, String name) {
        this.name = name;

        Reference<qdb_ts_column_info[]> columns =
            new Reference<qdb_ts_column_info[]>();
        int err = qdb.ts_list_columns(session.handle(), this.name, columns);
        QdbExceptionFactory.throwIfError(err);
        this.columns = columns.value;

        // Keep track of the columns that are part of this table, so
        // we can later look them up.
        this.columnOffsets = new HashMap(this.columns.length);
        for (int i = 0; i < this.columns.length; ++i) {
            this.columnOffsets.put(this.columns[i].name, i);
        }
    }

    /**
     * Initializes new writer for a timeseries table.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries table name. Must already exist.
     */
    public static QdbTimeSeriesWriter writer(QdbSession session, String name) {
        return new QdbTimeSeriesWriter(session,
                                       new QdbTimeSeriesTable(session, name));
    }

    /**
     * Initializes new writer for a timeseries table that periodically flushes
     * its local cache.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries table name. Must already exist.
     */
    public static QdbAutoFlushTimeSeriesWriter autoFlushWriter(QdbSession session, String name) {
        return new QdbAutoFlushTimeSeriesWriter(session,
                                                new QdbTimeSeriesTable(session, name));
    }

    /**
     * Initializes new writer for a timeseries table that periodically flushes
     * its local cache.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries table name. Must already exist.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public static QdbAutoFlushTimeSeriesWriter autoFlushWriter(QdbSession session, String name, long threshold) {
        return new QdbAutoFlushTimeSeriesWriter(session,
                                                new QdbTimeSeriesTable(session, name),
                                                threshold);
    }

    /**
     * Returns the timeseries table name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Returns internal representation of columns, for internal use
     * only.
     */
    public qdb_ts_column_info[] getColumnInfo() {
        return this.columns;
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
}
