package net.quasardb.qdb.ts;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import net.quasardb.qdb.jni.*;

/**
 * Holds information about a single column.
 */
public class Column {
    protected String name;
    protected Value.Type type;

    /**
     * A blob column.
     */
    public static class Blob extends Column {
        public Blob(String name) {
            super(name, Value.Type.BLOB);
        }
    }

    /**
     * A double precision column.
     */
    public static class Double extends Column {
        public Double(String name) {
            super(name, Value.Type.DOUBLE);
        }
    }

    /**
     * A 64 bit integer column.
     */
    public static class Int64 extends Column {
        public Int64(String name) {
            super(name, Value.Type.INT64);
        }
    }


    /**
     * A timestamp column.
     */
    public static class Timestamp extends Column {
        public Timestamp(String name) {
            super(name, Value.Type.TIMESTAMP);
        }
    }

    /**
     * Create a column with a certain type.
     */
    public Column(String name, Value.Type type) {
        this.name = name;
        this.type = type;
    }

    protected Column(String name, int type) {
        this(name, Value.Type.fromInt(type));
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
    public Value.Type getType() {
        return this.type;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Column)) return false;
        Column rhs = (Column)obj;

        return this.name.compareTo(rhs.name) == 0 && this.type == rhs.type;
    }

    public String toString() {
        return "Column (name: '" + this.name + "', type: " + this.type + ")";
    }
}
