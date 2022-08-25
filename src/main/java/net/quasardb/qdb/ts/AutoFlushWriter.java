package net.quasardb.qdb.ts;

import java.io.IOException;
import java.io.Flushable;
import java.lang.AutoCloseable;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


    private static final Logger logger = LoggerFactory.getLogger(Writer.class);

    long counter;
    long threshold;

    /**
     * Initialize a new auto-flushing batch writer with a default threshold of 50000 rows.
     *
     * @param session Active connection with the QdbCluster
     * @param options Batch writer options
     */
    protected AutoFlushWriter(Session session, Writer.Options options) {
        this(session, 50000, options);
    }

    /**
     * Initialize a new auto-flushing batch writer.
     *
     * @param session Active connection with the QdbCluster
     * @param threshold The amount of rows to keep in local buffer before automatic flushing occurs.
     * @param options Writer options
     */
    protected AutoFlushWriter(Session session, long threshold, Writer.Options options) {
        super(session, options);

        logger.info("Initializing AutoFlushWriter with threshold {}, pushMode {}", threshold, options);

        this.counter = 0;
        this.threshold = threshold;
    }

    @Override
    public void append(Table table, Timespec timestamp, Value[] values) throws IOException {
        super.append(table, timestamp, values);

        if (++counter >= threshold) {
            try {
                this.flush();
            } finally {
                this.counter = 0;
            }
        }
    }
}
