import java.util.*;
import java.time.*;
import java.lang.Exception;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.InvalidIteratorException;
import net.quasardb.qdb.exception.InvalidArgumentException;

public class WriterTest {

    private Session s;

    public enum DeduplicationStyle {
        NO_DEDUPLICATION,
        FULL_DEDUPLICATION,
        COLUMN_WISE_DEDUPLICATION;
    };

    @BeforeEach
    public void setup() {
        this.s = TestUtils.createSession();
    }

    @AfterEach
    public void teardown() {
        this.s.purgeAll(300000);
        this.s.close();
        this.s = null;
    }

    static Stream<Arguments> pushModeProvider() {
        return Stream.of(
                         Arguments.of(Writer.PushMode.NORMAL),
                         Arguments.of(Writer.PushMode.TRUNCATE),
                         Arguments.of(Writer.PushMode.FAST)
                         // Arguments.of(Writer.PushMode.ASYNC)
                         );
    }

    static Stream<Arguments> deduplicationStyleProvider() {
        return Stream.of(Arguments.of(DeduplicationStyle.NO_DEDUPLICATION),
                         Arguments.of(DeduplicationStyle.FULL_DEDUPLICATION),
                         Arguments.of(DeduplicationStyle.COLUMN_WISE_DEDUPLICATION));
    }

    static Stream<Arguments> columnTypeProvider() {
        return Stream.of(Arguments.of(Column.Type.DOUBLE),
                         Arguments.of(Column.Type.INT64),
                         Arguments.of(Column.Type.TIMESTAMP),
                         Arguments.of(Column.Type.STRING),
                         Arguments.of(Column.Type.SYMBOL),
                         Arguments.of(Column.Type.BLOB));
    }

    static Stream<Arguments> columnTypesProvider() {
        // Need to wrap in Object[] because otherwise the array will be
        // automatically expanded.
        return Stream.of(Arguments.of(new Object[]{new Column.Type[]{Column.Type.DOUBLE,
                                                                     Column.Type.INT64}}),
                         Arguments.of(new Object[]{new Column.Type[]{Column.Type.INT64,
                                                                     Column.Type.BLOB}}),
                         Arguments.of(new Object[]{new Column.Type[]{Column.Type.BLOB,
                                                                     Column.Type.TIMESTAMP}}),
                         Arguments.of(new Object[]{new Column.Type[]{Column.Type.TIMESTAMP,
                                                                     Column.Type.STRING}}),
                         Arguments.of(new Object[]{new Column.Type[]{Column.Type.STRING,
                                                                     Column.Type.DOUBLE}}));
    }

    static Stream<Arguments> combineStreams(Stream<Arguments> lhs,
                                            Stream<Arguments> rhs) {
        Arguments[] lhsArgs = lhs.toArray(Arguments[]::new);
        Arguments[] rhsArgs = rhs.toArray(Arguments[]::new);

        ArrayList<Arguments> ret = new ArrayList<Arguments>();
        for (Arguments lhsArg : lhsArgs) {
            for (Arguments rhsArg : rhsArgs) {
                ret.add(Arguments.of(lhsArg.get()[0],
                                     rhsArg.get()[0]));
            }
        }

        return ret.stream();
    }

    static Stream<Arguments> pushModeAndColumnTypeProvider() {
        return combineStreams(pushModeProvider(),
                              columnTypeProvider());
    }

    static Stream<Arguments> deduplicationStyleAndColumnTypesProvider() {
        return combineStreams(deduplicationStyleProvider(),
                              columnTypesProvider());
    }


    static boolean isTruncatePushMode(Writer.PushMode mode) {
        return mode == Writer.PushMode.TRUNCATE;

    }

    static boolean isTruncatePushMode(Object arg) {
        assert(arg instanceof Writer.PushMode);
        return isTruncatePushMode((Writer.PushMode)(arg));

    }

    static boolean isTruncatePushMode(Object[] args) {
        return isTruncatePushMode(args[0]);
    }

    static Stream<Arguments> truncatePushModeAndColumnTypeProvider() {
        return pushModeAndColumnTypeProvider().filter(args -> isTruncatePushMode(args.get()));
    }

    static Stream<Arguments> pushModeAndColumnTypesProvider() {
        return combineStreams(pushModeProvider(),
                              columnTypesProvider());
    }

    Writer writerByPushMode(Writer.PushMode mode) {
        switch (mode) {
        case NORMAL:
            return Writer.builder(this.s).normalPush().build();
        case FAST:
            return Writer.builder(this.s).fastPush().build();
        case ASYNC:
            return Writer.builder(this.s).asyncPush().build();
        case TRUNCATE:
            return Writer.builder(this.s).truncatePush().build();
        };

        throw new IllegalArgumentException("Invalid push mode: " + mode.toString());
    }

    void pushmodeAwareFlush(Writer w) throws Exception {
        w.flush();

        // if (w.pushMode() == Writer.PushMode.ASYNC) {
        //     Thread.sleep(8000);
        // }
    }


    @ParameterizedTest
    @EnumSource(Writer.PushMode.class)
    public void canGetWriter(Writer.PushMode mode) throws Exception {
        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Writer w = writerByPushMode(mode);
        w.close();
   }


    @ParameterizedTest
    @EnumSource(Writer.PushMode.class)
    public void canFlushWriter(Writer.PushMode mode) throws Exception {
        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Writer w = writerByPushMode(mode);
        try {
            w.flush();
        } finally {
            w.close();
        }
    }

    @ParameterizedTest
    @EnumSource(Writer.PushMode.class)
    public void canCloseWriter(Writer.PushMode mode) throws Exception {
        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Writer w = writerByPushMode(mode);
        w.close();
    }


    @ParameterizedTest
    @MethodSource("pushModeAndColumnTypeProvider")
    public void canCalculateSize(Writer.PushMode mode, Column.Type columnType) throws Exception {
        int COLUMN_COUNT = 3;
        int ROW_COUNT = 7;

        String alias = TestUtils.createUniqueAlias();
        Column[] definition = TestUtils.generateTableColumns(columnType, COLUMN_COUNT);
        Table t = TestUtils.createTable(definition);


        Writer writer = writerByPushMode(mode);
        assertEquals(writer.size(), 0);

        WritableRow[] rows = TestUtils.generateTableRows(definition, ROW_COUNT);

        for (int i = 0; i < rows.length; ++i) {
            writer.append(t, rows[i]);
            assertEquals((i + 1) * COLUMN_COUNT, writer.size());
        }

        try {
            pushmodeAwareFlush(writer);
        } finally {
            writer.close();
        }

        assertEquals(writer.size(), 0);
    }

    @ParameterizedTest
    @MethodSource("pushModeAndColumnTypeProvider")
    public void canInsertRow(Writer.PushMode mode, Column.Type columnType) throws Exception {
        String alias = TestUtils.createUniqueAlias();
        Column[] definition = TestUtils.generateTableColumns(columnType, 1);

        Table t = TestUtils.createTable(definition);

        Value[] values = {
            TestUtils.generateRandomValueByType(columnType)
        };

        Timespec timestamp = Timespec.now();

        WritableRow writeRow = new WritableRow(timestamp, values);

        Writer writer = writerByPushMode(mode);
        try {
            writer.append(t, writeRow);
            pushmodeAwareFlush(writer);
        } finally {
            writer.close();
        }

        TimeRange[] ranges = {
            new TimeRange(timestamp,
                          timestamp.plusNanos(1))
        };

        Reader r = Table.reader(s, t, ranges);

        try {
            assertTrue(r.hasNext());
            Row readRow = r.next();

            assertEquals(readRow, writeRow);
        } finally {
            r.close();
        }
    }

    @ParameterizedTest
    @MethodSource("pushModeAndColumnTypeProvider")
    public void canInsertMultipleRows(Writer.PushMode mode, Column.Type columnType) throws Exception {
        String alias = TestUtils.createUniqueAlias();
        Column[] definition = TestUtils.generateTableColumns(columnType, 1);

        Table t = TestUtils.createTable(definition);
        Writer writer = writerByPushMode(mode);

        try {
            int ROW_COUNT = 250;

            WritableRow[] rows = new WritableRow[ROW_COUNT];
            for (int i = 0; i < rows.length; ++i) {
                rows[i] =
                    new WritableRow (LocalDateTime.now(),
                                     new Value[] {
                                         TestUtils.generateRandomValueByType(32, columnType)});
                writer.append(t, rows[i]);
            }

            pushmodeAwareFlush(writer);

            TimeRange[] ranges = {
                new TimeRange(rows[0].getTimestamp(),
                              new Timespec(rows[(rows.length - 1)].getTimestamp().asLocalDateTime().plusNanos(1)))
            };

            Reader r = Table.reader(s, t, ranges);
            try {
                WritableRow[] readRows = r.stream().toArray(WritableRow[]::new);
                assertArrayEquals(rows, readRows);
            } finally {
                r.close();
            }

        } finally {
            writer.close();
        }
    }

    @ParameterizedTest
    @MethodSource("pushModeAndColumnTypeProvider")
    public void canInsertNullRows(Writer.PushMode mode, Column.Type columnType) throws Exception {
        double NULL_CHANCE = 0.5;
        String alias = TestUtils.createUniqueAlias();
        Column[] definition = TestUtils.generateTableColumns(columnType, 1);

        Table t = TestUtils.createTable(definition);
        Writer writer = writerByPushMode(mode);

        try {
            int ROW_COUNT = 250;

            WritableRow[] rows = new WritableRow[ROW_COUNT];
            for (int i = 0; i < rows.length; ++i) {
                rows[i] =
                    new WritableRow (LocalDateTime.now(),
                                     new Value[] {
                                         TestUtils.generateRandomValueByType(32, columnType, NULL_CHANCE)});
                writer.append(t, rows[i]);
            }

            pushmodeAwareFlush(writer);

            TimeRange[] ranges = {
                new TimeRange(rows[0].getTimestamp(),
                              new Timespec(rows[(rows.length - 1)].getTimestamp().asLocalDateTime().plusNanos(1)))
            };

            WritableRow[] readRows = TestUtils.readRows(s, t, ranges);
            assertArrayEquals(rows, readRows);
        } finally {
            writer.close();
        }
    }


    @ParameterizedTest
    @MethodSource("pushModeAndColumnTypesProvider")
    public void canInsertMultipleColumns(Writer.PushMode mode,
                                         Column.Type[] columnTypes) throws Exception {
        String alias = TestUtils.createUniqueAlias();
        Column[] definition = TestUtils.generateTableColumns(columnTypes);

        Table t = TestUtils.createTable(definition);
        Writer writer = writerByPushMode(mode);

        try {
            int ROW_COUNT = 2500;

            WritableRow[] rows = new WritableRow[ROW_COUNT];
            for (int i = 0; i < rows.length; ++i) {
                Value[] vs = Arrays.stream(columnTypes)
                    .map((columnType) -> {
                            return TestUtils.generateRandomValueByType(32, columnType);
                        })
                    .toArray(Value[]::new);

                rows[i] =
                    new WritableRow (LocalDateTime.now(), vs);
                writer.append(t, rows[i]);
            }

            pushmodeAwareFlush(writer);

            TimeRange[] ranges = {
                new TimeRange(rows[0].getTimestamp(),
                              new Timespec(rows[(rows.length - 1)].getTimestamp().asLocalDateTime().plusNanos(1)))
            };

            WritableRow[] readRows = TestUtils.readRows(s, t, ranges);
            assertArrayEquals(rows, readRows);
        } finally {
            writer.close();
        }
    }

    @ParameterizedTest
    @MethodSource("pushModeAndColumnTypesProvider")
    public void canInsertMultipleTables(Writer.PushMode mode,
                                        Column.Type[] columnTypes) throws Exception {
        Column[] definition1 = TestUtils.generateTableColumns(columnTypes);
        Column[] definition2 = TestUtils.generateTableColumns(columnTypes);

        Table t1 = TestUtils.createTable(definition1);
        Table t2 = TestUtils.createTable(definition2);

        assert(t1.getName() != t2.getName());

        Writer writer = writerByPushMode(mode);

        try {
            int ROW_COUNT = 250;


            // We really want the two tables to "overlap", so we split a large array here
            // and dividie it over two smaller arrays
            WritableRow[] rows = TestUtils.generateTableRows(definition1, ROW_COUNT * 2);
            WritableRow[] rows1 = new WritableRow[ROW_COUNT];
            WritableRow[] rows2 = new WritableRow[ROW_COUNT];

            for (int i = 0; i < rows.length; ++i) {
                if (i % 2 == 0) {
                    rows1[i / 2] = rows[i];
                } else {
                    rows2[i / 2] = rows[i];
                };
            };

            for (int i = 0; i < ROW_COUNT; ++i) {
                writer.append(t1, rows1[i]);
                writer.append(t2, rows2[i]);
            }

            pushmodeAwareFlush(writer);

            WritableRow[] readRows1 =
                TestUtils.readRows(s, t1, TestUtils.singleRangeFromRows(rows1));

            WritableRow[] readRows2 =
                TestUtils.readRows(s, t2, TestUtils.singleRangeFromRows(rows2));

            assertArrayEquals(rows1, readRows1);
            assertArrayEquals(rows2, readRows2);

        } finally {
            writer.close();
        }
    }

    @ParameterizedTest
    @MethodSource("pushModeAndColumnTypeProvider")
    public void canFlushTwice(Writer.PushMode mode, Column.Type columnType) throws Exception {
        Column[] definition = TestUtils.generateTableColumns(columnType, 1);

        Table t = TestUtils.createTable(definition);
        Writer writer = writerByPushMode(mode);

        try {
            int ROW_COUNT = 1000;

            Timespec ts = Timespec.now();
            WritableRow[] rows = new WritableRow[ROW_COUNT];
            for (int i = 0; i < rows.length; ++i) {

                // Because we're testing the truncate batch writer, we want to make 100% sure
                // there are no rows with identical timestamps, otherwise a second flush may
                // actually truncate data from a prior flush.
                ts = ts.plusNanos(1);
                rows[i] =
                    new WritableRow (ts,
                                     new Value[] {
                                         TestUtils.generateRandomValueByType(32, columnType)});
            }

            // Insert and flush the first half of the rows
            for (int i = 0; i < 500; ++i) {
                writer.append(t, rows[i]);
            }
            pushmodeAwareFlush(writer);

            // Second half
            for (int i = 500; i < rows.length; ++i) {
                writer.append(t, rows[i]);
            }
            pushmodeAwareFlush(writer);

            TimeRange[] ranges = {
                new TimeRange(rows[0].getTimestamp(),
                              new Timespec(rows[(rows.length - 1)].getTimestamp().asLocalDateTime().plusNanos(1)))
            };

            Reader r = Table.reader(s, t, ranges);
            try {
                WritableRow[] readRows = r.stream().toArray(WritableRow[]::new);
                assertArrayEquals(rows, readRows);
            } finally {
                r.close();
            }
        } finally {
            writer.close();
        }
    }


    @ParameterizedTest
    @MethodSource("truncatePushModeAndColumnTypeProvider")
    public void canTruncate(Writer.PushMode mode, Column.Type columnType) throws Exception {
        Column[] definition = TestUtils.generateTableColumns(columnType, 1);

        Table t = TestUtils.createTable(definition);
        Writer writer = writerByPushMode(mode);

        try {
            int ROW_COUNT = 250;

            Timespec ts = Timespec.now();
            WritableRow[] rows = new WritableRow[ROW_COUNT];
            for (int i = 0; i < rows.length; ++i) {

                // Because we're testing the truncate batch writer, we want to make 100% sure
                // there are no rows with identical timestamps, otherwise a second flush may
                // actually truncate data from a prior flush.
                ts = ts.plusNanos(1);
                rows[i] =
                    new WritableRow (ts,
                                     new Value[] {
                                         TestUtils.generateRandomValueByType(32, columnType)});
            }

            // Insert and flush the time
            for (WritableRow row : rows) {
                writer.append(t, row);
            }
            pushmodeAwareFlush(writer);

            // Second time is the *exact* same rows
            for (WritableRow row : rows) {
                writer.append(t, row);
            }
            pushmodeAwareFlush(writer);

            TimeRange[] ranges = {
                new TimeRange(rows[0].getTimestamp(),
                              new Timespec(rows[(rows.length - 1)].getTimestamp().asLocalDateTime().plusNanos(1)))
            };
        } finally {
            writer.close();
        }
    }

    /**
     * Sets deduplication options based on style. If column-wise, uses column with offset 0
     * to deduplicate.
     */
    static private Writer.Builder setDeduplicationOptions(Column[] columns, Writer.Builder builder,  DeduplicationStyle deduplicationStyle) {
        switch (deduplicationStyle) {
        case FULL_DEDUPLICATION:
            return builder.dropDuplicates();
        case COLUMN_WISE_DEDUPLICATION:
            return builder.dropDuplicates(new String[]{columns[0].getName()});
        case NO_DEDUPLICATION:
            break;
        };

        return builder;
    }

    @ParameterizedTest
    @MethodSource("deduplicationStyleAndColumnTypesProvider")
    public void canDropDuplicates(DeduplicationStyle deduplicationStyle, Column.Type[] columnTypes) throws Exception {
        assertEquals(columnTypes.length, 2);

        /////
        //
        // Does a 'standard' deduplication, that is, it only deduplicates when
        // *all* columns have identical values.
        //

        Column[] definition = TestUtils.generateTableColumns(columnTypes);
        Table t = TestUtils.createTable(definition);
        Writer.Builder builder = Writer.builder(this.s).fastPush();
        builder = setDeduplicationOptions(definition, builder, deduplicationStyle);
        Writer writer = builder.build();

        try {
            int ROW_COUNT = 10;
            Timespec ts = Timespec.now();
            List<WritableRow> inputRows = Arrays.asList(TestUtils.generateTableRows(definition, ROW_COUNT));
            Collections.sort(inputRows);

            ArrayList<WritableRow> allRows = new ArrayList<WritableRow>();

            // Generate and insert "base" dataset
            for (WritableRow row : inputRows) {
                writer.append(t, row);
            }
            allRows.addAll(inputRows);
            Collections.sort(allRows);

            pushmodeAwareFlush(writer);

            // Base test: we can read our rows back after writing
            {
                List<WritableRow> readRows = Arrays.asList(TestUtils.readRows(s, t, TestUtils.singleRangeFromRows(allRows)));
                assertIterableEquals(inputRows, readRows);
            }

            ////
            // Test case one -- insert pure duplicates
            //

            for (WritableRow row : inputRows) {
                writer.append(t, row);
            }
            allRows.addAll(inputRows);
            Collections.sort(allRows);

            pushmodeAwareFlush(writer);

            // Verify behavior based on deduplication style

            {
                List<WritableRow> readRows = Arrays.asList(TestUtils.readRows(s, t, TestUtils.singleRangeFromRows(allRows)));
                Collections.sort(readRows);

                switch (deduplicationStyle) {
                case COLUMN_WISE_DEDUPLICATION:
                case FULL_DEDUPLICATION:
                    assertIterableEquals(inputRows, readRows);
                    break;
                case NO_DEDUPLICATION:
                    assertIterableEquals(allRows, readRows);
                    break;
                };
            }


            ////
            // Test case two -- insert semi-duplicates
            //
            // We mutate about half of the rows here, with the $timestamp *always* being
            // duplicated. This means that behavior for all three deduplication styles
            // will be different, as we use the $timestamp column as the identifier for
            // column-wise deduplication.
            ArrayList<WritableRow> mutatedRows = new ArrayList<WritableRow>();
            ArrayList<WritableRow> noMutatedRows = new ArrayList<WritableRow>();

            for (WritableRow row : inputRows) {
                boolean shouldMutate = TestUtils.randomBoolean();

                if (shouldMutate) {
                    // Use column with offset 1, because column with offset 0 is used for
                    // deduplication
                    row = TestUtils.mutateRow(row, 1);
                    mutatedRows.add(new WritableRow(row));
                } else {
                    noMutatedRows.add(new WritableRow(row));
                }

                writer.append(t, row);
            }
            pushmodeAwareFlush(writer);

            {
                List<WritableRow> readRows = Arrays.asList(TestUtils.readRows(s, t, TestUtils.singleRangeFromRows(allRows)));

                ArrayList<WritableRow> expected = new ArrayList();

                switch (deduplicationStyle) {
                case NO_DEDUPLICATION:
                    // No deduplication: we expect *all* rows to be visible, so just
                    // expect everything
                    expected.addAll(inputRows);
                    expected.addAll(inputRows);
                    expected.addAll(mutatedRows);
                    expected.addAll(noMutatedRows);
                    assertEquals(expected.size(), ROW_COUNT * 3);
                    break;

                case COLUMN_WISE_DEDUPLICATION:
                    // Column-wise deduplication: since all rows share the same $timestamp,
                    // we expect no rows to be added.
                    expected.addAll(inputRows);
                    break;

                case FULL_DEDUPLICATION:
                    // Full dedupication: we only expect the rows that were mutated to be
                    // added.
                    expected.addAll(inputRows);
                    expected.addAll(mutatedRows);

                    break;
                };

                Collections.sort(readRows);
                Collections.sort(expected);

                assertEquals(expected.size(), readRows.size());
                assertIterableEquals(expected, readRows);
            }

        } finally {
            writer.close();
        }
    }
}
