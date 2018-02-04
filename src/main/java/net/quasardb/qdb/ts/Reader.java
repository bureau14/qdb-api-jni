package net.quasardb.qdb.ts;

import java.io.IOException;
import java.lang.AutoCloseable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import java.nio.channels.SeekableByteChannel;
import java.util.*;

import net.quasardb.qdb.exception.ExceptionFactory;
import net.quasardb.qdb.exception.InvalidArgumentException;
import net.quasardb.qdb.exception.InvalidIteratorException;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;

/**
 * High-performance bulk reader for a QuasarDB timeseries table. This class follows the
 * general Iterator pattern, and allows you to scan entire timeseries tables in bulk.
 */
public class Reader implements AutoCloseable, Iterator<Row> {
    Session session;
    Table table;
    Long localTable;
    Reference<Row> next;

    protected Reader(Session session, Table table, FilteredRange[] ranges) {
        if (ranges.length <= 0) {
            throw new InvalidArgumentException("Reader requires at least one FilteredRange to read");
        }

        this.session = session;
        this.table = table;
        this.next = new Reference<Row>();

        Reference<Long> theLocalTable = new Reference<Long>();
        int err = qdb.ts_local_table_init(this.session.handle(), table.getName(), table.getColumns(), theLocalTable);
        ExceptionFactory.throwIfError(err);

        this.localTable = theLocalTable.get();

        err = qdb.ts_table_get_ranges(this.localTable, ranges);
        ExceptionFactory.throwIfError(err);
    }

    /**
     * @return The underlying table that is being written to.
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
        int err = qdb.ts_table_next_row(this.localTable, this.table.getColumns(), this.next);
        ExceptionFactory.throwIfError(err);
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

    /**
     * Check whether there is another row available for reading or not. When this
     * function returns true, it is safe to call {@link #next}.
     *
     * @return Returns true when another row is available for reading.
     */
    public boolean hasNext() {
        this.maybeReadNext();

        return !(this.next.isEmpty());
    }

    /**
     * Modifies internal state to move forward to the next row. Make sure to check
     * whether it is safe to read the next row using {@link #hasNext}.
     *
     * @throws InvalidIteratorException Thrown when the iterator has reached the end
     *                                  and no next row is available.
     * @return The next row.
     */
    public Row next() {
        this.maybeReadNext();

        if (this.hasNext() == false) {
            throw new InvalidIteratorException();
        }

        return this.next.pop();
    }
}
