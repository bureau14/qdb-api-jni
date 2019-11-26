package net.quasardb.common;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.*;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;

public class TestUtils {
    private static long n = 1;
    public static final String CLUSTER_URI = "qdb://127.0.0.1:28360";

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

    public static Table createTable(Column[] columns) throws IOException {
        return createTable(createSession(), columns);
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

    public static Value generateRandomValueByType(int complexity, Value.Type valueType) {
        switch (valueType) {
        case INT64:
            return Value.createInt64(randomInt64());
        case DOUBLE:
            return Value.createDouble(randomDouble());
        case TIMESTAMP:
            return Value.createTimestamp(randomTimestamp());
        case BLOB:
            return Value.createSafeBlob(createSampleData(complexity));
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
     * Generate table rows.
     *
     * @param cols       Describes the table layout
     * @param complexity Arbitrary complexity variable that is used when generating data. E.g. for blobs,
     *                   this denotes the size of the blob value being generated.
     */
    public static WritableRow[] generateTableRows(Column[] cols, int complexity, int count) {
        // Generate that returns entire rows with an appropriate value for each column.
        Supplier<Value[]> valueGen =
            (() ->
             Arrays.stream(cols)
             .map(Column::getType)
             .map((Value.Type valueType) -> {
                     return TestUtils.generateRandomValueByType(complexity, valueType);
                 })
             .toArray(Value[]::new));


        return Stream.generate(valueGen)
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
}
