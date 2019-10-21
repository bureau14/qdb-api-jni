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
     * Access to the underlying values of this row.
     */
    public Value[] getValues() {
        return this.values;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Row)) return false;
        Row rhs = (Row)obj;

        return Arrays.equals(this.getValues(), rhs.getValues());
    }

    public String toString() {
        return "Row (values = " + Arrays.toString(this.values) + ")";
    }
}
