package net.quasardb.qdb.ts;

import java.io.IOException;
import java.io.Serializable;
import java.io.Flushable;
import java.lang.AutoCloseable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import java.nio.channels.SeekableByteChannel;
import java.util.*;

import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.ExceptionFactory;
import net.quasardb.qdb.exception.InvalidArgumentException;
import net.quasardb.qdb.jni.*;

/**
 * Represents a timeseries table.
 */
public class Table implements Serializable {
    String name;
    Column[] columns;
    Map <String, Integer> columnOffsets;

    /**
     * Initialize a new timeseries table.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries name. Must already exist.
     */
    Table(Session session, String name) {
        this.name = name;

        Reference<Column[]> columns =
            new Reference<Column[]>();
        int err = qdb.ts_list_columns(session.handle(), this.name, columns);
        ExceptionFactory.throwIfError(err);
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
    public static Writer writer(Session session, String name) {
        return new Writer(session,
                          new Table(session, name));
    }

    /**
     * Initializes new writer for a timeseries table that periodically flushes
     * its local cache.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries table name. Must already exist.
     */
    public static AutoFlushWriter autoFlushWriter(Session session, String name) {
        return new AutoFlushWriter(session,
                                   new Table(session, name));
    }

    /**
     * Initializes new writer for a timeseries table that periodically flushes
     * its local cache.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries table name. Must already exist.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public static AutoFlushWriter autoFlushWriter(Session session, String name, long threshold) {
        return new AutoFlushWriter(session,
                                   new Table(session, name),
                                   threshold);
    }

    /**
     * Initializes new reader for a timeseries table.
     *
     * @param session Active connection with the QdbCluster
     * @param name    Timeseries table name. Must already exist.
     * @param ranges  Filtered time ranges to look for.
     */
    public static Reader reader(Session session, String name, FilteredRange[] ranges) {
        return new Reader (session,
                           new Table(session, name),
                           ranges);
    }

    /**
     * Initializes new reader for a timeseries table.
     *
     * @param session Active connection with the QdbCluster
     * @param name    Timeseries table name. Must already exist.
     * @param ranges  Time ranges to look for.
     */
    public static Reader reader(Session session, String name, TimeRange[] ranges) {
        return reader(session, name,
                      Arrays
                      .stream(ranges)
                      .map(FilteredRange::new)
                      .toArray(FilteredRange[]::new));
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
    public Column[] getColumnInfo() {
        return this.columns;
    }

    /**
     * Utility function that looks up a column's index by its id. The first
     * column starts with 0.
     *
     * @param id String identifier of the column.
     * @return The index of the column inside the timeseries table definition.
     */
    public int columnIndexById (String id) {
        Integer offset = this.columnOffsets.get(id);
        if (offset == null) {
            throw new InvalidArgumentException();
        }

        return offset.intValue();
    }
}
