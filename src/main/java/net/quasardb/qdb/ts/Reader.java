package net.quasardb.qdb.ts;

import java.io.IOException;
import java.lang.AutoCloseable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import java.nio.channels.SeekableByteChannel;
import java.util.*;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;

/**
 * Represents a timeseries table.
 */
public class Reader implements AutoCloseable, Iterator<Row> {
    QdbSession session;
    Table table;
    Long localTable;
    Reference<Row> next;

    public Reader(QdbSession session, Table table, FilteredRange[] ranges) {
        if (ranges.length <= 0) {
            throw new QdbInvalidArgumentException("Reader requires at least one FilteredRange to read");
        }

        this.session = session;
        this.table = table;
        this.next = new Reference<Row>();

        Reference<Long> theLocalTable = new Reference<Long>();
        int err = qdb.ts_local_table_init(this.session.handle(), table.getName(), table.getColumnInfo(), theLocalTable);
        QdbExceptionFactory.throwIfError(err);

        this.localTable = theLocalTable.get();

        err = qdb.ts_table_get_ranges(this.localTable, ranges);
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Returns the underlying table that is being written to.
     */
    public Table getTable() {
        return this.table;
    }

    /**
     * Cleans up the internal representation of the local table.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            this.close();
        } finally {
            super.finalize();
        }
    }

    /**
     * Reads the next row from local table. Transparently updates the local
     * reference to the internal row.
     */
    private void readNext() {
        int err = qdb.ts_table_next_row(this.localTable, this.table.getColumnInfo(), this.next);
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Reads the next row from local table when appropriate.
     */
    private void maybeReadNext() {
        if (this.next.isEmpty()) {
            this.readNext();
        }
    }

    /**
     * Closes the timeseries table and local cache so that memory can be reclaimed.
     */
    public void close() throws IOException {
        qdb.ts_local_table_release(this.session.handle(), this.localTable);
        this.localTable = null;
    }

    public boolean hasNext() {
        this.maybeReadNext();

        return !(this.next.isEmpty());
    }

    /**
     * Modifies internal state to move forward to the next row.
     */
    public Row next() {
        this.maybeReadNext();

        if (this.hasNext() == false) {
            throw new QdbInvalidIteratorException();
        }

        return this.next.pop();
    }
}
