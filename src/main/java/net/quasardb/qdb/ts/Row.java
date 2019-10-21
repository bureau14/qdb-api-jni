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
public class Row implements Serializable {

    protected Timespec timestamp;
    protected Value[] values;

    /**
     * @brief Row without timestamp
     * @param values All values for this row.
     *
     * When querying data from the database, a row might not have a timestamp. This
     * function can be used to construct these rows.
     */
    public Row(Value[] values) {
        this.values = values;
    }

    /**
     * @brief Row with timestamp
     * @param timestamp The Valid Time for the row. This timestamp will be the primary index
     *                  that quasardb stores this row under.
     * @param values All values for this row.
     */
    public Row(Timespec timestamp, Value[] values) {
        this.timestamp = timestamp;
        this.values = values;
    }

    /**
     * @brief Row with timestamp
     * @param timestamp The Valid Time for the row. This timestamp will be the primary index
     *                  that quasardb stores this row under.
     * @param values All values for this row.
     */
    public Row(LocalDateTime timestamp, Value[] values) {
        this(new Timespec(timestamp), values);
    }

    /**
     * @brief Row with timestamp
     * @param timestamp The Valid Time for the row. This timestamp will be the primary index
     *                  that quasardb stores this row under.
     * @param values All values for this row.
     */
    public Row(Timestamp timestamp, Value[] values) {
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

    /**
     * @brief Access to the underlying values of this row.
     */
    public Value[] getValues() {
        return this.values;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Row)) return false;
        Row rhs = (Row)obj;

        if (this.timestamp != null) {
            if (rhs.getTimestamp() != null) return false;

            if (this.getTimestamp().equals(rhs.getTimestamp()) == false) return false;
        }

        return Arrays.equals(this.getValues(), rhs.getValues());
    }

    public String toString() {
        return "Row (timestamp: " + (this.timestamp != null ? this.timestamp.toString() : "NULL") + ", values = " + Arrays.toString(this.values) + ")";
    }
}
