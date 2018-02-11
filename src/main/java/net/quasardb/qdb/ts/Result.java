package net.quasardb.qdb.ts;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * The result of a Query
 */
public final class Result {

    /**
     * The tables that are part of the Query result. A single Query can
     * return multiple tables.
     **/
    public Table[] tables;

    /**
     * Represents a Table that is part of the Query result.
     */
    public static class Table {
        /**
         * The name of this table.
         */
        public String name;

        /**
         * The columns identifiers of this Table.
         */
        public String[] columns;

        /**
         * A array of all rows. Contains exactly one value per column.
         */
        public Value[][] rows;

        public String toString() {
            return "Table (name: " + this.name + ", columns: " + Arrays.toString(this.columns) + ", rows: " + Arrays.deepToString(this.rows) + ")";
        }
    }

    /**
     * Create a new empty result.
     */
    public Result() {
    }

    /**
     * Create a new result from result tables.
     *
     * @param tables An array of tables that the Result describes.
     */
    public Result(Table[] tables) {
        this.tables = tables;
    }

    /**
     * Access to a String representation of this Result.
     */
    public String toString() {
        return "Result (tables: " + Arrays.toString(this.tables) + ")";
    }

    /**
     * Provides stream-based access.
     */
    public Stream<Table> stream() {
        return Arrays.stream(this.tables);
    }
}
