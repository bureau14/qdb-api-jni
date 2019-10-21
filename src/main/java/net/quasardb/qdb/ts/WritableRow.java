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
     * @brief Row with timestamp
     * @param timestamp The Valid Time for the row. This timestamp will be the primary index
     *                  that quasardb stores this row under.
     * @param values All values for this row.
     */
    public WritableRow(Timespec timestamp, Value[] values) {
        super(values);

        this.timestamp = timestamp;
    }

    /**
     * @brief Row with timestamp
     * @param timestamp The Valid Time for the row. This timestamp will be the primary index
     *                  that quasardb stores this row under.
     * @param values All values for this row.
     */
    public WritableRow(LocalDateTime timestamp, Value[] values) {
        this(new Timespec(timestamp), values);
    }

    /**
     * @brief Row with timestamp
     * @param timestamp The Valid Time for the row. This timestamp will be the primary index
     *                  that quasardb stores this row under.
     * @param values All values for this row.
     */
    public WritableRow(Timestamp timestamp, Value[] values) {
        this(new Timespec(timestamp), values);
    }

    /**
     * @brief Access to the timestamp of this row.
     * @returns The timestamp, or null if this row does not have a timestamp associated (in case
     *          of a query result, for example).
     */
    public Timespec getTimestamp() {
        return this.timestamp;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) return false;
        if (!(obj instanceof WritableRow)) return false;

        WritableRow rhs = (WritableRow)obj;
        return this.timestamp.equals(rhs.getTimestamp());
    }

    public String toString() {
        return "WritableRow (timestamp: " + this.timestamp.toString() + ", values = " + Arrays.toString(this.values) + ")";
    }
}
