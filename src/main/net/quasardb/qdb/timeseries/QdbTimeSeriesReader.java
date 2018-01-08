package net.quasardb.qdb;

import java.io.IOException;
import java.lang.AutoCloseable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a timeseries table.
 */
public class QdbTimeSeriesReader implements AutoCloseable, Iterator<QdbTimeSeriesRow> {
    QdbSession session;
    QdbTimeSeriesTable table;
    Long localTable;
    Reference<QdbTimeSeriesRow> next;

    public QdbTimeSeriesReader(QdbSession session, QdbTimeSeriesTable table, QdbFilteredRange[] ranges) {
        if (ranges.length <= 0) {
            throw new QdbInvalidArgumentException("QdbTimeSeriesReader requires at least one QdbFilteredRange to read");
        }

        this.session = session;
        this.table = table;
        this.next = new Reference<QdbTimeSeriesRow>();

        Reference<Long> theLocalTable = new Reference<Long>();
        int err = qdb.ts_local_table_init(this.session.handle(), table.getName(), table.getColumnInfo(), theLocalTable);
        QdbExceptionFactory.throwIfError(err);

        this.localTable = theLocalTable.value;

        err = qdb.ts_table_get_ranges(this.localTable, ranges);
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Returns the underlying table that is being written to.
     */
    public QdbTimeSeriesTable getTable() {
        return this.table;
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
     * Reads the next row from local table. Transparently updates the local
     * reference to the internal row.
     */
    private void readNext() {
        System.out.println("5 readNext, this.next = " + this.next.get());
        int err = qdb.ts_table_next_row(this.localTable, this.table.getColumnInfo(), this.next);
        QdbExceptionFactory.throwIfError(err);
        System.out.println("6 readNext, this.next = " + this.next.get());
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
        System.out.println("1 hasNext, this.next = " + this.next.get());
        this.maybeReadNext();
        System.out.println("2 hasNext, this.next = " + this.next.get());

        return !(this.next.isEmpty());
    }

    /**
     * Modifies internal state to move forward to the next row.
     */
    public QdbTimeSeriesRow next() {
        System.out.println("3 next, this.next = " + this.next.get());
        this.maybeReadNext();

        System.out.println("4 next, this.next = " + this.next.get());

        if (this.hasNext() == false) {
            throw new QdbInvalidIteratorException();
        }

        return this.next.pop();
    }
}
