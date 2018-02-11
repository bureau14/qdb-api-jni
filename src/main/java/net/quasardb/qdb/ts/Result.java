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
     * Represents a single Result row.
     */
    public static class Row {
        /**
         * The name of the table the Row is from.
         */
        public String tableName;


        /**
         * The column identifiers of the Table the Row is from.
         */
        public String[] columns;

        /**
         * The values of this row. Contains exactly one value per column.
         */
        public Value[] values;

        /**
         * Constructor.
         *
         * @param tableName The name of the Table this Row is from.
         * @param columns The column identifiers of the Table this Row is from.
         * @param values The values of this row.
         **/
        protected Row(String tableName, String[] columns, Value[] values) {
            this.tableName = tableName;
            this.columns = columns;
            this.values = values;
        }

        public String toString() {
            return "Row (tableName: " + this.tableName + ", columns: " + Arrays.toString(this.columns) + ", values: " + Arrays.toString(this.values) + ")";
        }
    }

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

        /**
         * Flattens this Table by coercing the columns/values into Rows.
         */
        public Row[] flatten() {
            Row[] result = new Row[this.rows.length];

            for (int i = 0; i < this.rows.length; ++i) {
                result[i] = new Row(this.name, this.columns, this.rows[i]);
            }

            return result;
        }

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
    public Stream<Row> stream() {
        return
            Arrays.stream(this.tables)
            .map(Table::flatten)
            .flatMap(Arrays::stream);
    }
}
