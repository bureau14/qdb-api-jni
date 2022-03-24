package net.quasardb.qdb.ts;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import net.quasardb.qdb.jni.*;

/**
 * Holds information about a single column.
 */
public class Column {
    protected Type type;
    protected String name;
    protected String symbolTable;

    public enum Type {
        UNINITIALIZED(Constants.qdb_ts_column_uninitialized),
        DOUBLE(Constants.qdb_ts_column_double),
        BLOB(Constants.qdb_ts_column_blob),
        STRING(Constants.qdb_ts_column_string),
        INT64(Constants.qdb_ts_column_int64),
        TIMESTAMP(Constants.qdb_ts_column_timestamp),
        SYMBOL(Constants.qdb_ts_column_symbol)
        ;

        protected final int value;
        Type(int type) {
            this.value = type;
        }

        public int asInt() {
            return this.value;
        }

        public Value.Type asValueType() {
            switch (this) {
            case DOUBLE:
                return Value.Type.DOUBLE;
            case INT64:
                return Value.Type.INT64;
            case TIMESTAMP:
                return Value.Type.TIMESTAMP;
            case BLOB:
                return Value.Type.BLOB;
            case SYMBOL:
                //! *FALLTHROUGH*
            case STRING:
                return Value.Type.STRING;
            }

            return Value.Type.UNINITIALIZED;
        }

        public static Type fromInt(int type) {
            switch(type) {
            case Constants.qdb_ts_column_double:
                return Type.DOUBLE;

            case Constants.qdb_ts_column_blob:
                return Type.BLOB;

            case Constants.qdb_ts_column_string:
                return Type.STRING;

            case Constants.qdb_ts_column_int64:
                return Type.INT64;

            case Constants.qdb_ts_column_timestamp:
                return Type.TIMESTAMP;

            case Constants.qdb_ts_column_symbol:
                return Type.SYMBOL;
            }


            return Type.UNINITIALIZED;
        }
    }

    /**
     * A blob column.
     */
    public static class Blob extends Column {
        public Blob(String name) {
            super(name, Column.Type.BLOB);
        }
    }

    /**
     * A string column.
     */
    public static class String_ extends Column {
        public String_(String name) {
            super(name, Column.Type.STRING);
        }
    }

    /**
     * A string column.
     */
    public static class Symbol extends Column {
        public Symbol(String name, String symbolTable) {
            super(name, Column.Type.SYMBOL, symbolTable);
        }
    }

    /**
     * A double precision column.
     */
    public static class Double extends Column {
        public Double(String name) {
            super(name, Column.Type.DOUBLE);
        }
    }

    /**
     * A 64 bit integer column.
     */
    public static class Int64 extends Column {
        public Int64(String name) {
            super(name, Column.Type.INT64);
        }
    }


    /**
     * A timestamp column.
     */
    public static class Timestamp extends Column {
        public Timestamp(String name) {
            super(name, Column.Type.TIMESTAMP);
        }
    }

    /**
     * Create a column with a symbol table argument. Assumes type == SYMBOL.
     */
    public Column(String name, Column.Type type, String symbolTable) {
        assert(type == Column.Type.SYMBOL);

        this.name = name;
        this.type = type;
        this.symbolTable = symbolTable;
    }

    /**
     * Create a column with a symbol table argument. Assumes type == SYMBOL.
     */
    public Column(String name, int type, String symbolTable) {
        this(name, Column.Type.fromInt(type), symbolTable);
    }

    /**
     * Create a column with a certain type.
     */
    public Column(String name, Column.Type type) {
        this.name = name;
        this.type = type;
        this.symbolTable = "";
    }

    protected Column(String name, int type) {
        this(name, Column.Type.fromInt(type));
    }

    /**
     * Provides access to the column's name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Provides access to the column's type.
     */
    public Column.Type getType() {
        return this.type;
    }

    /**
     * Provides access to the column's symbol table.
     */
    public String getSymbolTable() {
        return this.symbolTable;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Column)) return false;
        Column rhs = (Column)obj;

        return this.name.compareTo(rhs.name) == 0 && this.type == rhs.type;
    }

    public String toString() {
        if (this.type == Column.Type.SYMBOL) {
            return "Column (name: '" + this.name + "', type: " + this.type + ", symbolTable: " + this.symbolTable + ")";
        } else {
            return "Column (name: '" + this.name + "', type: " + this.type + ")";
        }
    }
}
