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
 *
 * As with Writer, usage of instances of this class is not thread-safe. Use an
 * AutFlushWriter instance per Thread in multi-threaded situations.
 */
public final class AutoFlushWriter extends Writer {

    long counter;
    long threshold;

    /**
     * Initialize a new auto-flushing timeseries table with a default threshold of 50000 rows.
     *
     * @param session Active connection with the QdbCluster
     * @param tables Timeseries tables we're writing to.
     */
    protected AutoFlushWriter(Session session, Table[] tables) {
        this(session, tables, 50000);
    }

    /**
     * Initialize a new auto-flushing timeseries table.
     *
     * @param session Active connection with the QdbCluster
     * @param tables Timeseries tables we're writing to.
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     */
    protected AutoFlushWriter(Session session, Table[] tables, long threshold) {
        super(session, tables);

        this.counter = 0;
        this.threshold = threshold;
    }

    @Override
    public void append(Integer offset, Timespec timestamp, Value[] values) throws IOException {
        super.append(offset, timestamp, values);

        if (++counter >= threshold) {
            try {
                this.flush();
            } finally {
                this.counter = 0;
            }
        }
    }
}
