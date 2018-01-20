package net.quasardb.qdb.ts;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import net.quasardb.qdb.jni.*;

public class Column {
    protected String name;
    protected Value.Type type;

    public static class Blob extends Column {
        public Blob(String name) {
            super(name, Value.Type.BLOB);
        }
    }

    public static class Double extends Column {
        public Double(String name) {
            super(name, Value.Type.DOUBLE);
        }
    }

    public static class Int64 extends Column {
        public Int64(String name) {
            super(name, Value.Type.INT64);
        }
    }


    public static class Timestamp extends Column {
        public Timestamp(String name) {
            super(name, Value.Type.TIMESTAMP);
        }
    }

    public Column(String name, Value.Type type) {
        this.name = name;
        this.type = type;
    }

    public Column(String name, int type) {
        this(name, Value.Type.fromInt(type));
    }

    public String getName() {
        return this.name;
    }

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
