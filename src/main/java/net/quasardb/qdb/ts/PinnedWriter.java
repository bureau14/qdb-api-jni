package net.quasardb.qdb.ts;

import java.io.IOException;
import java.io.Flushable;
import java.lang.AutoCloseable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import java.nio.channels.SeekableByteChannel;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.InputException;
import net.quasardb.qdb.exception.InvalidArgumentException;
import net.quasardb.qdb.exception.OutOfBoundsException;
import net.quasardb.qdb.jni.*;

/**
 * Experimental, high-performance bulk writer for a QuasarDB timeseries table.
 *
 * Usage of instances of this class is not thread-safe. Use a Writer
 * instance per Thread in multi-threaded situations.
 */
public class PinnedWriter extends Writer {

    private static final Logger logger = LoggerFactory.getLogger(PinnedWriter.class);

    protected PinnedWriter(Session session, Table[] tables) {
        super(session, tables);
    }

    protected PinnedWriter(Session session, Table[] tables, Writer.PushMode mode) {
        super(session, tables, mode);
    }
}
