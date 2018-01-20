package net.quasardb.qdb.ts;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.SeekableByteChannel;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a timeseries row.
 */
public final class Row implements Serializable {

    protected Timespec timestamp;
    protected Value[] values;

    public Row(Timespec timestamp, Value[] values) {
        this.timestamp = timestamp;
        this.values = values;
    }

    public Row(LocalDateTime timestamp, Value[] values) {
        this(new Timespec(timestamp), values);
    }

    public Row(Timestamp timestamp, Value[] values) {
        this(new Timespec(timestamp), values);
    }

    public Timespec getTimestamp() {
        return this.timestamp;
    }

    public Value[] getValues() {
        return this.values;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Row)) return false;
        Row rhs = (Row)obj;

        return
            this.getTimestamp().equals(rhs.getTimestamp()) &&
            Arrays.equals(this.getValues(), rhs.getValues());
    }

    public String toString() {
        return "Row (timestamp: " + this.timestamp.toString() + ", values = " + Arrays.toString(this.values) + ")";
    }
}
