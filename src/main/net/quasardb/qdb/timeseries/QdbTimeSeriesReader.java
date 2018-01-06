package net.quasardb.qdb;

import java.io.IOException;
import java.io.Flushable;
import java.lang.AutoCloseable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a timeseries table.
 */
public class QdbTimeSeriesReader {
    QdbSession session;
    QdbTimeSeriesTable table;
    Long localTable;

    public QdbTimeSeriesReader(QdbSession session, QdbTimeSeriesTable table) {
        this.session = session;
        this.table = table;

        Reference<Long> theLocalTable = new Reference<Long>();
        int err = qdb.ts_local_table_init(this.session.handle(), table.getName(), table.getColumnInfo(), theLocalTable);
        QdbExceptionFactory.throwIfError(err);

        this.localTable = theLocalTable.value;
    }
}
