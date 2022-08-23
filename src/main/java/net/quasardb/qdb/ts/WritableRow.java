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
public final class WritableRow extends Row implements Serializable {

    protected Timespec timestamp;

    /**
     * Row with timestamp
     *
     * @param timestamp The Valid Time for the row. This timestamp will be the primary index
     *                  that quasardb stores this row under.
     * @param values All values for this row.
     */
    public WritableRow(Timespec timestamp, Value[] values) {
        super(values);

        this.timestamp = timestamp;
    }

    /**
     * Row with timestamp
     *
     * @param timestamp The Valid Time for the row. This timestamp will be the primary index
     *                  that quasardb stores this row under.
     * @param values All values for this row.
     */
    public WritableRow(LocalDateTime timestamp, Value[] values) {
        this(new Timespec(timestamp), values);
    }

    /**
     * Row with timestamp
     *
     * @param timestamp The Valid Time for the row. This timestamp will be the primary index
     *                  that quasardb stores this row under.
     * @param values All values for this row.
     */
    public WritableRow(Timestamp timestamp, Value[] values) {
        this(new Timespec(timestamp), values);
    }

    /**
     * Creates a new WritableRow out of an existing row, that is, copies the underlying
     * representation.
     */
    public WritableRow(WritableRow row) {
        super((Row)(row));

        this.timestamp = new Timespec(row.getTimestamp());
    }

    /**
     * Access to the timestamp of this row.
     *
     * @return The timestamp, or null if this row does not have a timestamp associated (in case
     *         of a query result, for example).
     */
    public Timespec getTimestamp() {
        return this.timestamp;
    }

    /**
     * Set timestamp of this row.
     *
     * @param timestamp Timestamp to set
     */
    public void setTimestamp(Timespec timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Comparison-by-value operator.
     *
     * @return Returns true when the object representations are equal.
     */
    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) return false;
        if (!(obj instanceof WritableRow)) return false;

        WritableRow rhs = (WritableRow)obj;
        return this.timestamp.equals(rhs.getTimestamp());
    }

    /**
     * Comparable interface implementation.
     */
    @Override
    public int compareTo(Row rhs) {
        assert((rhs instanceof WritableRow) == true);
        WritableRow rhs_ = (WritableRow)rhs;

        int x = this.getTimestamp().compareTo(rhs_.getTimestamp());
        if (x != 0) {
            return x;
        };

        return super.compareTo(rhs);
    }

    /**
     * Determine whether this row has any null values.
     *
     * @return Returns true when at least one value is a null value.
     */

    public boolean hasNullValues() {
        return Arrays.stream(this.values).anyMatch(Value::isNull);
    }

    public String toString() {
        return "WritableRow (timestamp: " + this.timestamp.toString() + ", values = " + Arrays.toString(this.values) + ")";
    }
}
