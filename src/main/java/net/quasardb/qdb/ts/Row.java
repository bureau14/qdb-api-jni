package net.quasardb.qdb.ts;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.SeekableByteChannel;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a timeseries row.
 */
public class Row implements Serializable, Comparable<Row> {
    private static final Logger logger = LoggerFactory.getLogger(Row.class);

    protected Value[] values;

    /**
     * Row without timestamp
     *
     * @param values All values for this row.
     *
     * When querying data from the database, a row might not have a timestamp. This
     * function can be used to construct these rows.
     */
    public Row(Value[] values) {
        this.values = values;
    }

    /**
     * Creates row out of this row, that is, copies the underlying representation
     * into a new object.
     */
    public Row(Row row) {
        int n = row.values.length;
        this.values = new Value[n];

        for (int i = 0; i < n; ++i) {
            this.values[i] = new Value(row.values[i]);
        }
    }

    /**
     * Access to the underlying values of this row.
     */
    public Value[] getValues() {
        return this.values;
    }

    /**
     * Get a value with a certain offset.
     */
    public Value getValue(int n) {
        assert(n < this.values.length && n >= 0);
        return this.values[n];
    }

    /**
     * Update a value with a certain offset.
     */
    public void setValue(Value v, int n) {
        assert(n < this.values.length && n >= 0);
        this.values[n] = v;
    }

    /**
     * Returns number of values assigbed to this row.
     */
    public int size() {
        return this.values.length;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Row)) return false;
        Row rhs = (Row)obj;

        return Arrays.equals(this.getValues(), rhs.getValues());
    }

    @Override
    public int compareTo(Row rhs) {
        if (this.size() < rhs.size()) {
            return -1;
        } else if (this.size() > rhs.size()) {
            return 1;
        }

        for (int i = 0; i < this.size(); ++i) {
            int x = this.getValue(i).compareTo(rhs.getValue(i));

            if (x != 0) {
                return x;
            };
        };

        assert(this.equals(rhs) == true);
        return 0;
    };

    public String toString() {
        return "Row (values = " + Arrays.toString(this.values) + ")";
    }
}
