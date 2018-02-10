package net.quasardb.qdb.ts;

import java.util.Arrays;

/**
 * The result of a Query
 */
public final class Result {

    /**
     * The tables that are part of the Query result. A single Query can
     * return multiple tables.
     **/
    public Table[] tables;

    public static class Table {
        public String name;
        public String[] columns;
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
}
