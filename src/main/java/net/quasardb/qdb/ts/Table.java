package net.quasardb.qdb.ts;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.*;
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
    protected long shardSizeMillis;
    protected long shardSizeSecs;
    protected Column[] columns;
    Map <String, Integer> columnOffsets;

    /**
     * Initialize a new timeseries table.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries name. Must already exist.
     */
    public Table(Session session, String name) {
        this(Table.getColumns(session, name),
             Table.getShardSize(session, name),
             name);
    }

    /**
     * Initialize a new timeseries table.
     *
     * @param columns Table  columns
     * @param name Timeseries name. Must already exist.
     */
    public Table(Column[] columns, long shardSizeMillis, String name) {
        assert(columns != null);

        logger.debug("Table constructor, columns.length = {}", columns.length);
        logger.debug("Table constructor, columns = {}", Arrays.toString(columns));

        this.name = name;
        this.columns = columns;
        this.shardSizeMillis = shardSizeMillis;

        // The batch writer needs the shard size by seconds a lot, specifically,
        // so we also cache that here.
        this.shardSizeSecs = this.shardSizeMillis / 1000;

        // Keep track of the columns that are part of this table, so
        // we can later look them up.
        this.columnOffsets = new HashMap(this.columns.length);
        for (int i = 0; i < this.columns.length; ++i) {
            assert(this.columns[i] != null);
            this.columnOffsets.put(this.columns[i].name, i);
        }
    }

    /**
     * Creates a new table object with the exact same structure and shard size
     * as another table. This is an efficient way to initialize large sets of tables
     * that all share a common shard size / schema, as no lookups with the QuasarDB
     * cluster are performed.
     *
     * @param other Table to use the schema and shard size of
     * @param name  Name of the new table to be initialized.
     */
    static public Table likeOther(Table other, String name) {
        return new Table(other.columns, other.shardSizeMillis, name);
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
        qdb.ts_create(session.handle(),
                      name,
                      shardSize,
                      columns);

        logger.debug("created, columns: {}", Arrays.toString(columns));

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
        qdb.ts_remove(session.handle(), name);
    }

    /**
     * Remove existing timeseries table.
     *
     * @param session Active session with the QuasarDB cluster
     * @param table Table to remove
     */
    public static void remove(Session session, Table table) {
        remove(session, table.getName());
    }

    /**
     * Initializes new reader for a timeseries table that filters for a time range.
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
     * @param name    Timeseries table name. Must already exist.
     */
    public static Reader reader(Session session, String name) {
        return reader(session,
                      new Table(session, name));
    }

    /**
     * Initializes new reader for a timeseries table that filters for a time range.
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
     * Initializes new reader for a timeseries table,
     *
     * @param session Active session with the QuasarDB cluster.
     * @param table   Timeseries table.
     */
    public static Reader reader(Session session, Table table) {
        return new Reader (session,
                           table);
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
            logger.debug("Attaching tag {} to table {}", tag, tableName);
            qdb.attach_tag(session.handle(), tableName, tag);
        }
    }

    /**
     * Returns the table name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Returns this table's shard size (in milliseconds)
     */
    public long getShardSizeMillis() {
        return this.shardSizeMillis;
    }

    /**
     * Returns this table's shard size (in seconds)
     */
    public long getShardSize() {
        return this.shardSizeSecs;
    }

    /**
     * Returns the shard size (in milliseconds) of the table.
     */
    public static long getShardSizeMillis(Session session, Table table) {
        return table.getShardSizeMillis();
    }

    /**
     * Returns the shard size (in seconds) of the table.
     */
    public static long getShardSize(Session session, Table table) {
        return table.getShardSize();
    }

    /**
     * Returns the shard size (in milliseconds) of the table.
     */
    public static long getShardSize(Session session, String tableName) {
        return qdb.ts_shard_size(session.handle(), tableName);
    }


    /**
     * Returns column layout of table.
     *
     * @param session Active session with the QuasarDB cluster.
     * @param name Unique identifier for this timeseries table.
     */
    static public Column[] getColumns(Session session, String name) {
        Column[] ret = qdb.ts_list_columns(session.handle(), name);

        assert(ret != null);
        return ret;
    }

    /**
     * Returns column representation of this table.
     */
    public Column[] getColumns() {
        assert(this.columns != null);
        return this.columns;
    }

    /**
     * Returns reference to Column object for column with a certain name.
     *
     * @param name The name to search for.
     */
    public Column getColumnByName(String name) {
        for (int i = 0; i < this.columns.length; ++i) {
            if (this.columns[i].getName().equals(name)) {
                return this.columns[i];
            }
        }

        return null;
    }

    /**
     * Returns `true` if table has a column with the name.
     *
     * @param name The name to search for.
     */
    public boolean hasColumnWithName(String name) {
        return getColumnByName(name) != null;
    }

    /**
     * Returns the types of each of the columns.
     */
    public Column.Type[] getColumnTypes() {
        Column[] columns = this.getColumns();

        Column.Type[] columnTypes = new Column.Type[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            columnTypes[i] = columns[i].getType();
        }

        return columnTypes;
    }

    /**
     * Returns the types of the values that are used to represent data for each column.
     */
    public Value.Type[] getColumnTypesAsValueTypes() {
        Column.Type[] xs = getColumnTypes();
        Value.Type[] ret = new Value.Type[xs.length];

        for (int i = 0; i < xs.length; ++i) {
            ret[i] = xs[i].asValueType();
        }

        return ret;
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
            throw new InvalidArgumentException("Column '" + id + "' not found for this table: '" + this.name + "'");
        }

        return offset.intValue();
    }

    public String toString() {
        return "Table (name: " + this.name + ", columns: " + Arrays.toString(this.columns) + ")";
    }
}
