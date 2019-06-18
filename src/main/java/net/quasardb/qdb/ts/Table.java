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
import net.quasardb.qdb.jni.*;

/**
 * Represents a timeseries Table. Upon construction, actively resolves metadata
 * such as the associated Columns.
 *
 * Can also be used to construct new QuasarDB timeseries tables.
 */
public class Table implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(Table.class);
    final static long DEFAULT_SHARD_SIZE = 86400000;

    protected String name;
    protected Column[] columns;
    Map <String, Integer> columnOffsets;

    /**
     * Initialize a new timeseries table.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries name. Must already exist.
     */
    public Table(Session session, String name) {
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
     * Create new timeseries table by copying a 'skeleton' table's schema and using
     * the default shard size.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param name Unique identifier for this timeseries table.
     * @param skeleton Skeleton table's schema to be copied.
     * @return Reference to the newly created timeseries table.
     */
    static public Table create(Session session, String name, Table skeleton) {
        return create(session, name, skeleton, DEFAULT_SHARD_SIZE);
    }

    /**
     * Create new timeseries table by copying a 'skeleton' table's schema.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param name Unique identifier for this timeseries table.
     * @param skeleton Skeleton table's schema to be copied.
     * @param shardSize The size of the shards in ms.
     * @return Reference to the newly created timeseries table.
     */
    static public Table create(Session session, String name, Table skeleton, long shardSize) {
        return create(session, name, skeleton.columns, shardSize);
    }

    /**
     * Create new timeseries table with a default shard size.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param name Unique identifier for this timeseries table.
     * @param columns Column definitions of this table. The ordering of the array will persist
     *                through the table definition and cannot be changed after creation.
     * @return Reference to the newly created timeseries table.
     */
    static public Table create(Session session, String name, Column[] columns) {
        return create(session, name, columns, DEFAULT_SHARD_SIZE);
    }

    /**
     * Create new timeseries table.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param name Unique identifier for this timeseries table.
     * @param columns Column definitions of this table. The ordering of the array will persist
     *                through the table definition and cannot be changed after creation.
     * @param shardSize The size of the shards in ms.
     * @return Reference to the newly created timeseries table.
     */
    static public Table create(Session session, String name, Column[] columns, long shardSize) {
        logger.info("Creating new table {} with shard size: {}", name, shardSize);
        int err = qdb.ts_create(session.handle(),
                                name,
                                shardSize,
                                columns);

        ExceptionFactory.throwIfError(err);

        return new Table(session, name);
    }

    /**
     * Remove existing timeseries table.
     *
     * @param session Active session with the QuasarDB cluster
     * @param name Unique identifier for this timeseries table.
     */
    public static void remove(Session session, String name) {
        logger.info("Dropping table {}", name);
        int err = qdb.ts_remove(session.handle(), name);
        ExceptionFactory.throwIfError(err);
    }


    /**
     * Initializes new writer for a single timeseries table.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param name Timeseries table name. Must already exist.
     */
    public static Writer writer(Session session, String name) {
        return writer(session, new Table(session, name));
    }

    /**
     * Initializes new writer for a timeseries table.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param table Timeseries table.
     */
    public static Writer writer(Session session, Table table) {
        return Tables.writer(session, new Table[] {table});
    }

    /**
     * Initializes new writer for a single timeseries table using
     * high-speed buffered writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param name Timeseries table name. Must already exist.
     */
    public static Writer asyncWriter(Session session, String name) {
        return asyncWriter(session, new Table(session, name));
    }

    /**
     * Initializes new writer for a timeseries table using high-speed
     * buffered writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param table Timeseries table.
     */
    public static Writer asyncWriter(Session session, Table table) {
        return Tables.asyncWriter(session, new Table[] {table});
    }

    /**
     * Initializes new writer for a timeseries table that periodically flushes
     * its local cache.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param name Timeseries table name. Must already exist.
     */
    public static AutoFlushWriter autoFlushWriter(Session session, String name) {
        return autoFlushWriter(session, new Table(session, name));
    }

    /**
     * Initializes new writer for a timeseries table that periodically flushes
     * its local cache.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param table Timeseries table.
     */
    public static AutoFlushWriter autoFlushWriter(Session session, Table table) {
        return Tables.autoFlushWriter(session, new Table[] {table});
    }

    /**
     * Initializes new writer for a timeseries table that periodically flushes
     * its local cache.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param name Timeseries table name. Must already exist.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public static AutoFlushWriter autoFlushWriter(Session session, String name, long threshold) {
        return autoFlushWriter(session,
                               new Table(session, name),
                               threshold);
    }

    /**
     * Initializes new writer for a timeseries table that periodically flushes
     * its local cache.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param table Timeseries table.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public static AutoFlushWriter autoFlushWriter(Session session, Table table, long threshold) {
        return Tables.autoFlushWriter(session,
                                      new Table[] {table},
                                      threshold);
    }

    /**
     * Initializes new writer for a timeseries table that periodically flushes
     * its local cache, and makes use of high-speed buffered writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param name Timeseries table name. Must already exist.
     */
    public static AutoFlushWriter asyncAutoFlushWriter(Session session, String name) {
        return asyncAutoFlushWriter(session, new Table(session, name));
    }

    /**
     * Initializes new writer for a timeseries table that periodically flushes
     * its local cache, and makes use of high-speed buffered writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param table Timeseries table.
     */
    public static AutoFlushWriter asyncAutoFlushWriter(Session session, Table table) {
        return Tables.asyncAutoFlushWriter(session, new Table[] {table});
    }

    /**
     * Initializes new writer for a timeseries table that periodically flushes
     * its local cache, and makes use of high-speed buffered writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param name Timeseries table name. Must already exist.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public static AutoFlushWriter asyncAutoFlushWriter(Session session, String name, long threshold) {
        return asyncAutoFlushWriter(session,
                                    new Table(session, name),
                                    threshold);
    }

    /**
     * Initializes new writer for a timeseries table that periodically flushes
     * its local cache, and makes use of high-speed buffered writes.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param table Timeseries table.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    public static AutoFlushWriter asyncAutoFlushWriter(Session session, Table table, long threshold) {
        return Tables.asyncAutoFlushWriter(session,
                                           new Table[] {table},
                                           threshold);
    }

    /**
     * Initializes new reader for a timeseries table.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param name    Timeseries table name. Must already exist.
     * @param ranges  Time ranges to look for.
     */
    public static Reader reader(Session session, String name, TimeRange[] ranges) {
        return reader(session,
                      new Table(session, name),
                      ranges);
    }

    /**
     * Initializes new reader for a timeseries table.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param table   Timeseries table.
     * @param ranges  Time time ranges to look for.
     */
    public static Reader reader(Session session, Table table, TimeRange[] ranges) {
        return new Reader (session,
                           table,
                           ranges);
    }

    /**
     * Attaches a tag to an existing table.
     *
     * @param session Active session with the QuasarDB cluster
     * @param table   Timeseries table
     * @param tag     Tag to attach
     */
    public static void attachTag(Session session, Table table, String tag) {
        attachTag(session, table.getName(), tag);
    }

    /**
     * Attaches a tag to an existing table.
     *
     * @param session   Active session with the QuasarDB cluster
     * @param tableName Name of the timeseries table
     * @param tag       Tag to attach
     */
    public static void attachTag(Session session, String tableName, String tag) {
        attachTags(session, tableName, Arrays.asList(tag));
    }

    /**
     * Attaches tags to an existing table.
     *
     * @param session Active session with the QuasarDB cluster
     * @param table   Timeseries table
     * @param tags    Tags to attach
     */
    public static void attachTags(Session session, Table table, List<String> tags) {
        attachTags(session, table.getName(), tags);
    }

    /**
     * Attaches tags to an existing table.
     *
     * @param session   Active session with the QuasarDB cluster
     * @param tableName Name of the timeseries table
     * @param tags      Tags to attach
     */
    public static void attachTags(Session session, String tableName, List<String> tags) {
        for (String tag : tags) {
            int err = qdb.attach_tag(session.handle(), tableName, tag);
            ExceptionFactory.throwIfError(err);
        }
    }

    /**
     * Returns the timeseries table name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Returns column representation of this table.
     */
    public Column[] getColumns() {
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

    public String toString() {
        return "Table (name: " + this.name + ", columns: " + Arrays.toString(this.columns) + ")";
    }
}
