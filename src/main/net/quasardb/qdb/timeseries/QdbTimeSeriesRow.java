package net.quasardb.qdb;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.SeekableByteChannel;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a timeseries row.
 */
public final class QdbTimeSeriesRow implements Serializable {

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

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbTimeSeriesRow)) return false;
        QdbTimeSeriesRow rhs = (QdbTimeSeriesRow)obj;

        System.out.println("clomparing QdbTimeSeriesRow equalit!");

        return
            this.getTimestamp().equals(rhs.getTimestamp()) &&
            Arrays.equals(this.getValues(), rhs.getValues());
    }

    public String toString() {
        return "QdbTimeSeriesRow (timestamp: " + this.timestamp.toString() + ", values = " + Arrays.toString(this.values) + ")";
    }
}
