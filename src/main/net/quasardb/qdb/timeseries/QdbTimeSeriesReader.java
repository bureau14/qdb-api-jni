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


    QdbTimeSeriesRow next;
    QdbTimeSeriesRow prev;

    public QdbTimeSeriesReader(QdbSession session, QdbTimeSeriesTable table, QdbFilteredRange[] ranges) {
        if (ranges.length <= 0) {
            throw new QdbInvalidArgumentException("QdbTimeSeriesReader requires at least one QdbFilteredRange to read");
        }

        this.session = session;
        this.table = table;
        this.next = null;
        this.prev = QdbTimeSeriesRow.createNull(table.getColumnInfo().length);
        System.out.println("Creating NULL row, this.prev = " + this.prev.toString());

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
    public QdbTimeSeriesTable getTable() {
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
        assert(this.prev != null);
        assert(this.next == null);

        System.out.println("invoking .readNext(), prev = " + this.prev.toString());

        int err = qdb.ts_table_next_row(this.localTable, this.table.getColumnInfo(), this.prev);
        QdbExceptionFactory.throwIfError(err);

        System.out.println("after .readNext(), prev = " + this.prev.toString());

        this.next = this.prev;
        this.prev = null;
    }

    /**
     * When a new row is being read, the next and prev are being switched so that we
     * do not accidentally return the same value twice.
     */
    private void swapNext() {
        System.out.println("swapNext, prev = " + this.prev);
        System.out.println("swapNext, next = " + this.next);

        assert(this.prev == null);
        assert(this.next != null);

        this.prev = this.next;
        this.next = null;

    }

    /**
     * Reads the next row from local table when appropriate.
     */
    private void maybeReadNext() {
        if (this.next == null) {
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

        return this.next != null;
    }

    /**
     * Modifies internal state to move forward to the next row.
     */
    public QdbTimeSeriesRow next() {
        this.maybeReadNext();

        if (this.hasNext() == false) {
            throw new QdbInvalidIteratorException();
        }

        System.out.println("QdbTimeSeriesReader.next(), calling swapNext()");

        this.swapNext();

        return this.prev;
    }
}
