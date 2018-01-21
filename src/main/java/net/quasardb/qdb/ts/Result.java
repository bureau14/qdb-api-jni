package net.quasardb.qdb.ts;

import java.util.Arrays;

/**
 * The result of a Query
 */
public final class Result {

    public Table[] tables;

    public static class Table {
        public String name;
        public String[] columns;
        public Value[][] rows;

        public String toString() {
            return "Table (name: " + this.name + ", columns: " + Arrays.toString(this.columns) + ", rows: " + Arrays.deepToString(this.rows) + ")";
        }
    }

    public Result() {
    }

    public String toString() {
        return "Result (tables: " + Arrays.toString(this.tables) + ")";
    }
}
