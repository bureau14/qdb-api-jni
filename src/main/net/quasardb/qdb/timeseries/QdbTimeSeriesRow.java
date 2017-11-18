package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a timeseries row.
 */
public final class QdbTimeSeriesRow {

    private QdbTimespec timestamp;
    private QdbTimeSeriesValue[] values;

    public QdbTimeSeriesRow(QdbTimespec timestamp, QdbTimeSeriesValue[] values) {
        this.timestamp = timestamp;
        this.values = values;
    }

    public QdbTimeSeriesRow(LocalDateTime timestamp, QdbTimeSeriesValue[] values) {
        this(new QdbTimespec(timestamp), values);
    }

    public QdbTimeSeriesRow(Timestamp timestamp, QdbTimeSeriesValue[] values) {
        this(new QdbTimespec(timestamp), values);
    }

    public QdbTimespec getTimestamp() {
        return this.timestamp;
    }

    public QdbTimeSeriesValue[] getValues() {
        return this.values;
    }
}
