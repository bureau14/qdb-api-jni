package net.quasardb.qdb.ts;

import java.io.IOException;
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
 * High-performance bulk writer for a QuasarDB timeseries table.
 */
public class Writer implements AutoCloseable, Flushable {
    Session session;
    Long batchTable;

    /**
     * Maintains a cache of table offsets so we can easily look them up
     * later.
     */
    Map<String, Integer> tableOffsets;

    /**
     * Helper class to represent a table and column pair, which we
     * need because we need to lay out all columns as flat array.
     */
    public static class TableColumn {
        public String table;
        public String column;

        public TableColumn(String table, String column) {
            this.table = table;
            this.column = column;
        }

        public String toString() {
            return "TableColumn (table: " + this.table + ", column: " + this.column + ")";
        }
    }

    protected Writer(Session session, Table[] tables) {
        this.session = session;
        this.tableOffsets = new HashMap<String, Integer>();

        List<TableColumn> columns = new ArrayList<TableColumn>();

        for (Table table : tables) {
            this.tableOffsets.put(table.name, columns.size());

            for (Column column : table.columns) {
                columns.add(new TableColumn(table.name, column.name));
            }
        }

        TableColumn[] tableColumns = columns.toArray(new TableColumn[columns.size()]);
        Reference<Long> theBatchTable = new Reference<Long>();
        int err = qdb.ts_batch_table_init(this.session.handle(),
                                          tableColumns,
                                          theBatchTable);
        ExceptionFactory.throwIfError(err);

        this.batchTable = theBatchTable.value;
    }

    /**
     * Utility function that looks up a table's index with the batch being written
     * by its name. The first table starts with column 0, but depending upon the amount
     * of columns in other tables, it can influence the offset of the table within the batch.
     *
     * If possible, you are encouraged to cache this value so that recurring writes
     * of rows to the same table only do this lookup once.
     */
    public int tableIndexByName(String name) {
        Integer offset = this.tableOffsets.get(name);
        if (offset == null) {
            throw new InvalidArgumentException();
        }

        return offset.intValue();
    }

    /**
     * Cleans up the internal representation of the batch table.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            qdb.ts_batch_table_release(this.session.handle(), this.batchTable);
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
        qdb.ts_batch_table_release(this.session.handle(), this.batchTable);

        this.batchTable = null;
    }

    /**
     * Flush current local cache to server.
     */
    public void flush() throws IOException {
        int err = qdb.ts_batch_push(this.batchTable);
        ExceptionFactory.throwIfError(err);
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * @param offset Relative offset of the table inside the batch. Use #tableIndexByName
     *               to determine the appropriate value.
     * @param timestamp Timestamp of the row
     * @param values Values being inserted, mapped to columns by their relative offset.
     *
     * @see #tableIndexByName
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(Integer offset, Timespec timestamp, Value[] values) throws IOException {
        int err = qdb.ts_batch_table_row_append(this.batchTable, offset, timestamp, values);
        ExceptionFactory.throwIfError(err);
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * This function automatically looks up a table's offset by its name. For performance
     * reason, you are encouraged to manually invoke and cache the value of #tableIndexByName
     * whenever possible.
     *
     * @param tableName Name of the table to insert to.
     * @param timestamp Timestamp of the row
     * @param values Values being inserted, mapped to columns by their relative offset.
     *
     * @see #tableIndexByName
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(String tableName, Timespec timestamp, Value[] values) throws IOException {
        this.append(this.tableIndexByName(tableName),
                    timestamp,
                    values);
    }


    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * This is a convenience function that assumes only one table is being inserted
     * to and should not be used when inserts to multiple tables are being batched.
     *
     * @param timestamp Timestamp of the row
     * @param values Values being inserted, mapped to columns by their relative offset.
     *
     * @see #tableIndexByName
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(Timespec timestamp, Value[] values) throws IOException {
        this.append(0, timestamp, values);
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * @param offset Relative offset of the table inside the batch. Use #tableIndexByName
     *               to determine the appropriate value.
     * @param row Row being inserted.
     *
     * @see #tableIndexByName
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(Integer offset, Row row) throws IOException {
        this.append(offset,
                    row.getTimestamp(),
                    row.getValues());
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * This function automatically looks up a table's offset by its name. For performance
     * reason, you are encouraged to manually invoke and cache the value of #tableIndexByName
     * whenever possible.
     *
     * @param tableName Name of the table to insert to.
     * @param row Row being inserted.
     *
     * @see #tableIndexByName
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(String tableName, Row row) throws IOException {
        this.append(this.tableIndexByName(tableName), row);
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * This is a convenience function that assumes only one table is being inserted
     * to and should not be used when inserts to multiple tables are being batched.
     *
     * @param row Row being inserted.
     *
     * @see #tableIndexByName
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(Row row) throws IOException {
        this.append(0, row);
    }


    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * @param offset Relative offset of the table inside the batch. Use #tableIndexByName
     *               to determine the appropriate value.
     * @param timestamp Timestamp of the row
     * @param values Values being inserted, mapped to columns by their relative offset.
     *
     * @see #tableIndexByName
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(Integer offset, LocalDateTime timestamp, Value[] values) throws IOException {
        this.append(offset, new Timespec(timestamp), values);
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * This function automatically looks up a table's offset by its name. For performance
     * reason, you are encouraged to manually invoke and cache the value of #tableIndexByName
     * whenever possible.
     *
     * @param tableName Name of the table to insert to.
     * @param timestamp Timestamp of the row
     * @param values Values being inserted, mapped to columns by their relative offset.
     *
     * @see #tableIndexByName
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(String tableName, LocalDateTime timestamp, Value[] values) throws IOException {
        this.append(this.tableIndexByName(tableName), timestamp, values);
    }


    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * This is a convenience function that assumes only one table is being inserted
     * to and should not be used when inserts to multiple tables are being batched.
     *
     * @param timestamp Timestamp of the row
     * @param values Values being inserted, mapped to columns by their relative offset.
     *
     * @see #tableIndexByName
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(LocalDateTime timestamp, Value[] values) throws IOException {
        this.append(0, timestamp, values);
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * @param offset Relative offset of the table inside the batch. Use #tableIndexByName
     *               to determine the appropriate value.
     * @param timestamp Timestamp of the row
     * @param values Values being inserted, mapped to columns by their relative offset.
     *
     * @see #tableIndexByName
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(Integer offset, Timestamp timestamp, Value[] values) throws IOException {
        this.append(offset, new Timespec(timestamp), values);
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * This function automatically looks up a table's offset by its name. For performance
     * reason, you are encouraged to manually invoke and cache the value of #tableIndexByName
     * whenever possible.
     *
     * @param tableName Name of the table to insert to.
     * @param timestamp Timestamp of the row
     * @param values Values being inserted, mapped to columns by their relative offset.
     *
     * @see #tableIndexByName
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(String tableName, Timestamp timestamp, Value[] values) throws IOException {
        this.append(this.tableIndexByName(tableName), timestamp, values);
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     *
     * This is a convenience function that assumes only one table is being inserted
     * to and should not be used when inserts to multiple tables are being batched.
     *
     * @param timestamp Timestamp of the row
     * @param values Values being inserted, mapped to columns by their relative offset.
     *
     * @see #tableIndexByName
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(Timestamp timestamp, Value[] values) throws IOException {
        this.append(0, timestamp, values);
    }
}
