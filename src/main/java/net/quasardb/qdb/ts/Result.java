package net.quasardb.qdb.ts;

import java.util.Arrays;
import java.util.stream.Stream;
import java.util.HashMap;
import java.util.Map;

import java.io.Serializable;

/**
 * The result of a Query
 */
public final class Result implements Serializable {

    public String[] columns;
    public Row[] rows;

    /**
     * Create a new result from result tables.
     */
    public Result(String[] columns, Row[] rows) {
        this.columns = columns;
        this.rows = rows;
    }

    /**
     * Access to a String representation of this Result.
     */
    public String toString() {
        return "Result (columns: " + Arrays.toString(this.columns) + ", rows: " + Arrays.toString(this.rows) + ")";
    }

    /**
     * Provides stream-based access.
     */
    public Stream<Row> stream() {
        return Arrays.stream(this.rows);
    }
}
