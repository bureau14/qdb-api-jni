package net.quasardb.qdb.ts;

import java.io.IOException;
import java.io.Serializable;
import java.io.Flushable;
import java.lang.AutoCloseable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import java.nio.channels.SeekableByteChannel;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.ExceptionFactory;
import net.quasardb.qdb.exception.InvalidArgumentException;
import net.quasardb.qdb.exception.IncompatibleTypeException;
import net.quasardb.qdb.jni.*;

/**
 * Utility class to make working with multiple tables easier by bridging
 * between the #Table and #Writer class.
 *
 * It maintains its own internal array of tables and can be serialized.
 */
public class Tables implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(Tables.class);
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
     * Returns the number of tables in this collection.
     *
     * @return The number of tables in this collection.
     */
    public int size() {
        return this.tables.size();
    }

    /**
     * Returns true when the collection contains a table with a certain name.
     * This operation has O(N) complexity.
     *
     * @param tableName The tablename to search for.
     * @return True when the collection contains a table with the name.
     */
    public boolean hasTableWithName(String tableName) {
        for (Table t : this.tables) {
            if (t.getName().equals(tableName)) {
                return true;
            }
        }

        return false;
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
     * @param name Name of the table to add
     */
    public Tables add (Session session, String name) {
        logger.debug("Adding table to collection: {}", name);
        return add(new Table(session, name));
    }

    public static Tables ofTag (Session session, String tag) {
        logger.debug("Looking up all tables by tag: {}", tag);
        Reference<Long> iterator = new Reference<Long>();
        int err = qdb.tag_iterator_begin(session.handle(), tag, iterator);
        ExceptionFactory.throwIfError(err);

        Tables tables = new Tables();

        boolean hasNext = err == qdb_error.ok;
        final long handle = iterator.value;

        while (hasNext == true) {
            int type = qdb.tag_iterator_type(handle);
            String alias = qdb.tag_iterator_alias(handle);

            if (type != qdb_entry_type.timeseries) {
                throw new IncompatibleTypeException("Not a timeseries: " + alias);
            }

            tables = tables.add(session, alias);

            err = qdb.tag_iterator_next(handle);
            hasNext = err == qdb_error.ok;
            ExceptionFactory.throwIfError(err);
        }

        return tables;
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
     * Initializes new writer for timeseries table using high-speed
     * buffered writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries tables.
     */
    public static Writer asyncWriter (Session session, Tables tables) {
        return asyncWriter(session, tables.getTables());
    }

    /**
     * Initializes new writer for timeseries table using hgih-speed
     * buffered writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries tables.
     */
    public static Writer asyncWriter (Session session, Table[] tables) {
        return new Writer(session, tables, Writer.PushMode.ASYNC);
    }

    /**
     * Initializes new writer for timeseries tables that makes use of
     * in-place updates rather than copy-on-write. This is especially useful
     * when you do lots of small, incremental pushes, such as streaming
     * data.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries tables.
     */
    public static Writer fastWriter (Session session, Tables tables) {
        return fastWriter(session, tables.getTables());
    }

    /**
     * Initializes new writer for timeseries tables that makes use of
     * in-place updates rather than copy-on-write. This is especially useful
     * when you do lots of small, incremental pushes, such as streaming
     * data.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries tables.
     */
    public static Writer fastWriter (Session session, Table[] tables) {
        return new Writer(session, tables, Writer.PushMode.FAST);
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

    /**
     * Initializes new writer for timeseries tables that periodically flushes
     * its local cache, and makes use of high-speed buffered writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries tables to write to.
     */
    public static AutoFlushWriter asyncAutoFlushWriter(Session session, Tables tables) {
        return asyncAutoFlushWriter(session,
                                    tables.getTables());
    }

    /**
     * Initializes new writer for timeseries tables that periodically flushes
     * its local cache, and makes use of high-speed buffered writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries table.
     */
    public static AutoFlushWriter asyncAutoFlushWriter(Session session, Table[] tables) {
        return new AutoFlushWriter(session,
                                   tables,
                                   Writer.PushMode.ASYNC);
    }

    /**
     * Initializes new writer for timeseries tables that periodically flushes
     * its local cache, and makes use of high-speed buffered writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries tables to write to.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public static AutoFlushWriter asyncAutoFlushWriter(Session session, Tables tables, long threshold) {
        return asyncAutoFlushWriter(session,
                                    tables.getTables(),
                                    threshold);
    }

    /**
     * Initializes new writer for timeseries tables that periodically flushes
     * its local cache, and makes use of high-speed buffered writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries table.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public static AutoFlushWriter asyncAutoFlushWriter(Session session, Table[] tables, long threshold) {
        return new AutoFlushWriter(session,
                                   tables,
                                   threshold,
                                   Writer.PushMode.ASYNC);
    }

    /**
     * Initializes new writer for timeseries tables that periodically flushes
     * its local cache, and makes use of high-speed in-place writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries tables to write to.
     */
    public static AutoFlushWriter fastAutoFlushWriter(Session session, Tables tables) {
        return fastAutoFlushWriter(session,
                                    tables.getTables());
    }

    /**
     * Initializes new writer for timeseries tables that periodically flushes
     * its local cache, and makes use of high-speed in-place writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries table.
     */
    public static AutoFlushWriter fastAutoFlushWriter(Session session, Table[] tables) {
        return new AutoFlushWriter(session,
                                   tables,
                                   Writer.PushMode.FAST);
    }

    /**
     * Initializes new writer for timeseries tables that periodically flushes
     * its local cache, and makes use of high-speed in-place writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries tables to write to.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public static AutoFlushWriter fastAutoFlushWriter(Session session, Tables tables, long threshold) {
        return fastAutoFlushWriter(session,
                                    tables.getTables(),
                                    threshold);
    }

    /**
     * Initializes new writer for timeseries tables that periodically flushes
     * its local cache, and makes use of high-speed in-place writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param tables Timeseries table.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public static AutoFlushWriter fastAutoFlushWriter(Session session, Table[] tables, long threshold) {
        return new AutoFlushWriter(session,
                                   tables,
                                   threshold,
                                   Writer.PushMode.FAST);
    }


    public String toString() {
        return "Tables (tables: " + this.tables.toString() + ")";
    }
}
