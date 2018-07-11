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
 * Utility class to make working with multiple tables easier by bridging
 * between the #Table and #Writer class.
 *
 * It maintains its own internal array of tables and can be serialized.
 */
public class Tables implements Serializable {
    protected List<Table> tables;

    /**
     * Initialize empty collection of Tables.
     */
    public Tables () {
        this.tables = new ArrayList<Table> ();
    }

    /**
     * Initialize a collection of timeseries Tables.
     *
     * @param tables Initial collection.
     */
    public Tables (Table[] tables) {
        this.tables = Arrays.asList(tables);
    }

    /**
     * Provides access to the internal #Table collection.
     */
    protected Table[] getTables() {
        return this.tables.toArray(new Table[this.tables.size()]);
    }

    /**
     * Returns a copy of this collection with the new table added.
     *
     * @param table The table to add
     */
    public Tables add (Table table) {
        this.tables.add(table);
        return this;
    }

    /**
     * Returns a copy of this collection with the new table added.
     *
     * @param session Active session wit the QuasarDB cluster.
     * @param name
     */
    public Tables add (Session session, String name) {
        return add (new Table(session, name));
    }

    /**
     * Initializes new writer for timeseries table.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries tables.
     */
    public static Writer writer (Session session, Tables tables) {
        return writer(session, tables.getTables());
    }

    /**
     * Initializes new writer for timeseries table.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries tables.
     */
    public static Writer writer (Session session, Table[] tables) {
        return new Writer (session, tables);
    }

    /**
     * Initializes new writer for timeseries tables that periodically flushes
     * its local cache.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries tables to write to.
     */
    public static AutoFlushWriter autoFlushWriter(Session session, Tables tables) {
        return autoFlushWriter(session,
                               tables.getTables());
    }

    /**
     * Initializes new writer for timeseries tables that periodically flushes
     * its local cache.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries table.
     */
    public static AutoFlushWriter autoFlushWriter(Session session, Table[] tables) {
        return new AutoFlushWriter(session,
                                   tables);
    }

    /**
     * Initializes new writer for timeseries tables that periodically flushes
     * its local cache.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries tables to write to.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public static AutoFlushWriter autoFlushWriter(Session session, Tables tables, long threshold) {
        return autoFlushWriter(session,
                               tables.getTables(),
                               threshold);
    }

    /**
     * Initializes new writer for timeseries tables that periodically flushes
     * its local cache.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries table.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public static AutoFlushWriter autoFlushWriter(Session session, Table[] tables, long threshold) {
        return new AutoFlushWriter(session,
                                   tables,
                                   threshold);
    }

    public String toString() {
        return "Tables (tables: " + this.tables.toString() + ")";
    }
}
