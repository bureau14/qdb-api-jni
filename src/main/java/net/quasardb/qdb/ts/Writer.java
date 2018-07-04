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
    Long localTable;

    /**
     * Maintains a cache of table offsets so we can easily look them up
     * later.
     */
    Map<String, Integer> tableOffsets;

    /**
     * Helper class to represent a pair of table <-> columns, which we
     * need because we need to lay out all columns as flat array.
     */
    public static class TableColumn {
        public String table;
        public String column;

        public TableColumn(String table, String column) {
            this.table = table;
            this.column = column;
        }
    }

    protected Writer(Session session, Table[] tables) {
        this.session = session;

        List<TableColumn> columns = new ArrayList<TableColumn>();
        for (Table table : tables) {
            this.tableOffsets.put(table.name, columns.size());

            for (Column column : table.columns) {
                columns.add(new TableColumn(table.name, column.name));
            }
        }

        Reference<Long> theLocalTable = new Reference<Long>();
        int err = qdb.ts_batch_table_init(this.session.handle(),
                                          columns.toArray(new TableColumn[columns.size()]),
                                          theLocalTable);
        ExceptionFactory.throwIfError(err);

        this.localTable = theLocalTable.value;
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
        ExceptionFactory.throwIfError(err);
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(Row row) throws IOException {
        int err = qdb.ts_table_row_append(this.localTable, row.getTimestamp(), row.getValues());
        ExceptionFactory.throwIfError(err);
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(Timespec timestamp, Value[] value) throws IOException {
        this.append(new Row(timestamp, value));
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(LocalDateTime timestamp, Value[] value) throws IOException {
        this.append(new Row(timestamp, value));
    }

    /**
     * Append a new row to the local table cache. Should be periodically flushed,
     * unless an {@link AutoFlushWriter} is used.
     * @see #flush
     * @see Table#autoFlushWriter
     */
    public void append(Timestamp timestamp, Value[] value) throws IOException {
        this.append(new Row(timestamp, value));
    }
}
