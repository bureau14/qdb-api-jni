package net.quasardb.qdb.ts;

import java.io.IOException;
import java.io.Flushable;
import java.lang.AutoCloseable;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;

/**
 * An implementation of a Writer that automatically flushes the local cache when
 * a certain threshold has been reached.
 */
public final class AutoFlushWriter extends Writer {

    long counter;
    long threshold;

    /**
     * Initialize a new auto-flushing timeseries table with a default threshold of 50000 rows.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries name. Must already exist.
     */
    protected AutoFlushWriter(Session session, Table table) {
        this(session, table, 50000);
    }

    /**
     * Initialize a new auto-flushing timeseries table.
     *
     * @param session Active connection with the QdbCluster
     * @param name Timeseries name. Must already exist.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    protected AutoFlushWriter(Session session, Table table, long threshold) {
        super(session, table);

        this.counter = 0;
        this.threshold = threshold;
    }

    @Override
    public void append(Row row) throws IOException {
        super.append(row);

        if (++counter >= threshold) {
            try {
                this.flush();
            } finally {
                this.counter = 0;
            }
        }
    }
}
