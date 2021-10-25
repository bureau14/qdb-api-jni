package net.quasardb.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.util.stream.Stream;
import java.util.*;
import java.nio.ByteBuffer;
import java.util.function.Supplier;
import java.io.Serializable;

import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;

public class TestUtils {
    private static long n = 1;
    public static final String CLUSTER_URI = "qdb://127.0.0.1:2836";

    public static Session createSession() {
        return Session.connect(CLUSTER_URI);
    }

    public static Column[] generateTableColumns(int count) {
        return generateTableColumns(Value.Type.DOUBLE, count);
    }

    public static Column[] generateTableColumns(Value.Type valueType, int count) {
        return Stream.generate(TestUtils::createUniqueAlias)
            .limit(count)
            .map((alias) -> {
                    return new Column(alias, valueType);
                })
            .toArray(Column[]::new);
    }

    public static Column[] generateTableColumns(Value.Type[] valueTypes) {
        return Arrays.stream(valueTypes)
            .limit(valueTypes.length)
            .map((valueType) -> {
                    return new Column(createUniqueAlias(),
                                      valueType);
                })
            .toArray(Column[]::new);
    }

    public static Table createTable(Column[] columns) throws IOException {
        Session s = createSession();

        try {
            return createTable(s, columns);
        } finally {
            s.close();
        }
    }

    public static Table createTable(Session s, Column[] columns) throws IOException {
        return createTable(s, createUniqueAlias(), columns);
    }

    public static Table createTable(Session s, String tableName, Column[] columns) throws IOException {
        return Table.create(s, tableName, columns);
    }

    public static String createUniqueAlias() {
        return new String("a") + UUID.randomUUID().toString().replaceAll("-", "");

    }

    public static ByteBuffer createSampleData() {
        return createSampleData(32);
    }

    public static ByteBuffer createSampleData(int size) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        createSampleData(size, buffer);
        return buffer;
    }

    public static void createSampleData(int size, ByteBuffer buffer) {
        byte[] b = new byte[size];
        createSampleData(b);

        buffer.put(b);
        buffer.flip();
    }

    public static void createSampleData(byte[] b) {
        new Random(n++).nextBytes(b);
    }

    public static double randomDouble() {
        return new Random(n++).nextDouble();
    }

    public static long randomInt64() {
        return new Random(n++).nextLong();
    }

    public static Timespec randomTimespec() {
        return randomTimestamp();

    }

    public static Timespec randomTimestamp() {
        return new Timespec(new Random(n++).nextInt(),
                            new Random(n++).nextInt());

    }

    public static Value generateRandomValueByType(Value.Type valueType) {
        return generateRandomValueByType(32, valueType);
    }

    public static Value generateRandomValueByType(int complexity, Value.Type valueType) {
        return generateRandomValueByType(complexity, valueType, 0.0);
    }

    public static Value generateRandomValueByType(int complexity, Value.Type valueType, double nullChance) {

        if (new Random().nextDouble() >= nullChance) {

            switch (valueType) {
            case INT64:
                return Value.createInt64(randomInt64());
            case DOUBLE:
                return Value.createDouble(randomDouble());
            case TIMESTAMP:
                return Value.createTimestamp(randomTimestamp());
            case BLOB:
                return Value.createSafeBlob(createSampleData(complexity));
            case STRING:
                return Value.createString(createUniqueAlias());
            }
        }

        return Value.createNull();

    }

    /**
     * Generate table rows with standard complexity of 32
     */
    public static WritableRow[] generateTableRows(Column[] cols, int count) {
        return generateTableRows(cols, 32, count);
    }

    /**
     * Generate table rows without null values.
     *
     * @param cols       Describes the table layout
     * @param complexity Arbitrary complexity variable that is used when generating data. E.g. for blobs,
     *                   this denotes the size of the blob value being generated.
     */
    public static WritableRow[] generateTableRows(Column[] cols, int complexity, int count) {
        return generateTableRows(cols, complexity, count, 0.0);
    }

    /**
     * Generate table rows with random null values.
     *
     * @param cols       Describes the table layout
     * @param complexity Arbitrary complexity variable that is used when generating data. E.g. for blobs,
     *                   this denotes the size of the blob value being generated.
     * @param nullChance Chance a value is marked as null, 0 being 0% chance and 1 being 100% chance
     */
    public static WritableRow[] generateTableRows(Column[] cols, int complexity, int count, double nullChance) {
        // Generate that returns entire rows with an appropriate value for each column.
        Supplier<Value[]> valueGen =
            (() ->
             Arrays.stream(cols)
             .map(Column::getType)
             .map((Value.Type valueType) -> {
                     return TestUtils.generateRandomValueByType(complexity, valueType, nullChance);
                 })
             .toArray(Value[]::new));


        return Stream.generate(valueGen)
            .filter((xs) -> {
                    for (Value x : xs) {
                        if (!x.isNull()) {
                            return true;
                        }
                    }

                    // All nulls, which would mean an empty row which is not
                    // inserted.
                    return false;
                })
            .limit(count)
            .map((v) ->
                 new WritableRow(Timespec.now(),
                                 v))
            .toArray(WritableRow[]::new);
    }

    public static Table seedTable(Session s, Column[] cols, WritableRow[] rows) throws Exception {
        return seedTable(s, createUniqueAlias(), cols, rows);
    }

    public static Table seedTable(Session s, String tableName, Column[] cols, WritableRow[] rows) throws Exception {
        Table t = createTable(s, tableName, cols);
        Writer writer = Table.writer(s, t);

        for (WritableRow row : rows) {
            writer.append(row);
        }

        writer.flush();
        writer.close();

        return t;
    }

    /**
     * Generates a TimeRange from an array of rows. Assumes that all rows are sorted,
     * with the oldest row being first.
     */
    public static TimeRange rangeFromRows(WritableRow[] rows) {
        assert(rows.length >= 1);

        Timespec first = rows[0].getTimestamp();
        Timespec last = rows[(rows.length - 1)].getTimestamp();

        return new TimeRange(first,
                             last.plusNanos(1));
    }

    public static TimeRange[] rangesFromRows(WritableRow[] rows) {
        return Arrays.stream(rows)
            .map(WritableRow::getTimestamp)
            .map((t) -> {
                     return new TimeRange(t, t.plusNanos(1));
                })
            .toArray(TimeRange[]::new);
    }

    public static <T extends Serializable> byte[] serialize(T obj)
        throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        return baos.toByteArray();
    }

    public static <T extends Serializable> T deserialize(byte[] b, Class<T> cl)
        throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(b);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object o = ois.readObject();
        return cl.cast(o);
    }

    public static WritableRow[] readRows(Session s, Table t, TimeRange[] r) throws IOException {
        Reader x = Table.reader(s,t,r);
        try {
            return x.stream().toArray(WritableRow[]::new);
        } finally {
            x.close();
        }
    }

}
