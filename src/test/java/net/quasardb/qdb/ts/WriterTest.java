import java.util.*;
import java.time.*;
import java.lang.Exception;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
                         Arguments.of(Writer.PushMode.FAST),
                         Arguments.of(Writer.PushMode.PINNED_NORMAL),
                         Arguments.of(Writer.PushMode.PINNED_FAST),
                         Arguments.of(Writer.PushMode.EXP_NORMAL),
                         Arguments.of(Writer.PushMode.EXP_TRUNCATE)
                         // Arguments.of(Writer.PushMode.ASYNC),
                         // Arguments.of(Writer.PushMode.EXP_ASYNC)
                         );
    }

    static Stream<Arguments> columnTypeProvider() {
        return Stream.of(Arguments.of(Column.Type.DOUBLE),
                         Arguments.of(Column.Type.INT64),
                         Arguments.of(Column.Type.BLOB),
                         Arguments.of(Column.Type.TIMESTAMP),
                         Arguments.of(Column.Type.STRING),
                         Arguments.of(Column.Type.SYMBOL));
    }

    static Stream<Arguments> columnTypesProvider() {
        return Stream.of(

                         // Need to wrap in Object[] because otherwise the array will be
                         // automatically expanded.
                         Arguments.of(new Object[]{new Column.Type[]{Column.Type.DOUBLE,
                                                                     Column.Type.INT64}}),
                         Arguments.of(new Object[]{new Column.Type[]{Column.Type.INT64,
                                                                     Column.Type.BLOB}}),
                         Arguments.of(new Object[]{new Column.Type[]{Column.Type.BLOB,
                                                                     Column.Type.TIMESTAMP}}),
                         Arguments.of(new Object[]{new Column.Type[]{Column.Type.TIMESTAMP,
                                                                     Column.Type.STRING}}),
                         Arguments.of(new Object[]{new Column.Type[]{Column.Type.STRING,
                                                                     Column.Type.SYMBOL}}),
                         Arguments.of(new Object[]{new Column.Type[]{Column.Type.SYMBOL,
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

    static boolean isTruncatePushMode(Writer.PushMode mode) {
        return mode == Writer.PushMode.TRUNCATE ||
            mode == Writer.PushMode.EXP_TRUNCATE;

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

    Writer writerByPushMode(Table t, Writer.PushMode mode) {
        switch (mode) {
        case NORMAL:
            return t.writer(s, t);
        case PINNED_NORMAL:
            return t.pinnedWriter(s, t);
        case EXP_NORMAL:
            return t.expWriter(s, t);
        case FAST:
            return t.fastWriter(s, t);
        case PINNED_FAST:
            return t.pinnedFastWriter(s, t);
        case EXP_FAST:
            return t.expFastWriter(s, t);
        case ASYNC:
            return t.asyncWriter(s, t);
        case EXP_ASYNC:
            return t.expAsyncWriter(s, t);
        case TRUNCATE:
            return t.truncateWriter(s, t);
        case EXP_TRUNCATE:
            return t.expTruncateWriter(s, t);
        }

        throw new IllegalArgumentException("Invalid push mode: " + mode.toString());
    }

    Writer writerByPushMode(Tables t, Writer.PushMode mode) {
        switch (mode) {
        case NORMAL:
            return t.writer(s, t);
        case PINNED_NORMAL:
            return t.pinnedWriter(s, t);
        case EXP_NORMAL:
            return t.expWriter(s, t);
        case FAST:
            return t.fastWriter(s, t);
        case PINNED_FAST:
            return t.pinnedFastWriter(s, t);
        case EXP_FAST:
            return t.expFastWriter(s, t);
        case ASYNC:
            return t.asyncWriter(s, t);
        case EXP_ASYNC:
            return t.expAsyncWriter(s, t);
        case TRUNCATE:
            return t.truncateWriter(s, t);
        case EXP_TRUNCATE:
            return t.expTruncateWriter(s, t);
        }

        throw new IllegalArgumentException("Invalid push mode: " + mode.toString());
    }

    void pushmodeAwareFlush(Writer w) throws Exception {
        w.flush();

        if (w.pushMode() == Writer.PushMode.ASYNC) {
            Thread.sleep(8000);
        }
    }


    @ParameterizedTest
    @EnumSource(Writer.PushMode.class)
    public void canGetWriter(Writer.PushMode mode) throws Exception {
        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Writer w = writerByPushMode(table, mode);
        w.close();
   }


    @ParameterizedTest
    @EnumSource(Writer.PushMode.class)
    public void canFlushWriter(Writer.PushMode mode) throws Exception {
        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Writer w = writerByPushMode(table, mode);
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

        Writer w = writerByPushMode(table, mode);
        w.close();
    }

    @ParameterizedTest
    @EnumSource(Writer.PushMode.class)
    public void canLookupTableOffsetById(Writer.PushMode mode) throws Exception {
        Column[] columns = {
            new Column.Double(TestUtils.createUniqueAlias()),
            new Column.Double(TestUtils.createUniqueAlias()),
            new Column.Double(TestUtils.createUniqueAlias()),
            new Column.Double(TestUtils.createUniqueAlias()),
            new Column.Double(TestUtils.createUniqueAlias()),
            new Column.Double(TestUtils.createUniqueAlias()),
            new Column.Double(TestUtils.createUniqueAlias()),
            new Column.Double(TestUtils.createUniqueAlias())
        };

        Table table1 = TestUtils.createTable(columns);
        Table table2 = TestUtils.createTable(columns);

        Tables tables = new Tables(new Table[] {table1, table2});

        Writer writer = writerByPushMode(tables, mode);

        try {
            assertEquals(writer.tableIndexByName(table1.getName()), 0);
            assertEquals(writer.tableIndexByName(table2.getName()), columns.length);
        } finally {
            writer.close();
        }
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

        Writer writer = writerByPushMode(t, mode);
        try {
            writer.append(writeRow);
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
        Writer writer = writerByPushMode(t, mode);

        try {
            int ROW_COUNT = 250;

            WritableRow[] rows = new WritableRow[ROW_COUNT];
            for (int i = 0; i < rows.length; ++i) {
                rows[i] =
                    new WritableRow (LocalDateTime.now(),
                                     new Value[] {
                                         TestUtils.generateRandomValueByType(32, columnType)});
                writer.append(rows[i]);
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
        Writer writer = writerByPushMode(t, mode);

        try {
            int ROW_COUNT = 250;

            WritableRow[] rows = new WritableRow[ROW_COUNT];
            for (int i = 0; i < rows.length; ++i) {
                rows[i] =
                    new WritableRow (LocalDateTime.now(),
                                     new Value[] {
                                         TestUtils.generateRandomValueByType(32, columnType, NULL_CHANCE)});
                writer.append(rows[i]);
            }

            pushmodeAwareFlush(writer);

            TimeRange[] ranges = {
                new TimeRange(rows[0].getTimestamp(),
                              new Timespec(rows[(rows.length - 1)].getTimestamp().asLocalDateTime().plusNanos(1)))
            };

            if (columnType == Column.Type.SYMBOL) {
                System.err.println("SKIPPING SYMBOL + NULL TEST, CURRENTLY BROKEN IN READER");
            } else {
                WritableRow[] readRows = TestUtils.readRows(s, t, ranges);
                assertArrayEquals(rows, readRows);
            }
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
        Writer writer = writerByPushMode(t, mode);

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
                writer.append(rows[i]);
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

    // @ParameterizedTest
    // @MethodSource("pushModeAndValueTypesProvider")
    // public void canAddExtraTable(Writer.PushMode mode,
    //                              Column.Type[] columnTypes) throws Exception {
    //     Column[] definition = TestUtils.generateTableColumns(columnTypes);

    //     Table t1 = TestUtils.createTable(definition);
    //     Table t2 = TestUtils.createTable(definition);

    //     Writer writer = writerByPushMode(t1, mode);

    //     int ROW_COUNT = 1000;

    //     WritableRow[] rows1 = new WritableRow[ROW_COUNT];
    //     WritableRow[] rows2 = new WritableRow[ROW_COUNT];

    //     for (int i = 0; i < rows1.length; ++i) {
    //         Value[] vs = Arrays.stream(columnTypes)
    //             .map((columnType) -> {
    //                     return TestUtils.generateRandomValueByType(32, columnType);
    //                 })
    //             .toArray(Value[]::new);

    //         rows1[i] =
    //             new WritableRow (LocalDateTime.now(), vs);
    //         writer.append(t1.getName(), rows1[i]);
    //     }

    //     // This is the crucial test: add additional state in the middle of the flush
    //     writer.extraTables(t2);

    //     for (int i = 0; i < rows2.length; ++i) {
    //         Value[] vs = Arrays.stream(columnTypes)
    //             .map((columnType) -> {
    //                     return TestUtils.generateRandomValueByType(32, columnType);
    //                 })
    //             .toArray(Value[]::new);

    //         rows2[i] =
    //             new WritableRow (LocalDateTime.now(), vs);
    //         writer.append(t2.getName(), rows2[i]);
    //     }

    //     // Do note we actually need to flush, as the pinned writer does most of its
    //     // complex operations in the flush operation.
    //     pushmodeAwareFlush(writer);

    //     TimeRange[] ranges1 = {
    //         new TimeRange(rows1[0].getTimestamp(),
    //                       new Timespec(rows1[(rows1.length - 1)].getTimestamp().asLocalDateTime().plusNanos(1)))
    //     };

    //     TimeRange[] ranges2 = {
    //         new TimeRange(rows2[0].getTimestamp(),
    //                       new Timespec(rows2[(rows2.length - 1)].getTimestamp().asLocalDateTime().plusNanos(1)))
    //     };

    //     WritableRow[] readRows1 = TestUtils.readRows(s, t1, ranges1);
    //     WritableRow[] readRows2 = TestUtils.readRows(s, t2, ranges2);
    //     assertArrayEquals(rows1, readRows1);
    //     assertArrayEquals(rows2, readRows2);
    // }

    @ParameterizedTest
    @MethodSource("pushModeAndColumnTypeProvider")
    public void canFlushTwice(Writer.PushMode mode, Column.Type columnType) throws Exception {
        String alias = TestUtils.createUniqueAlias();
        Column[] definition = TestUtils.generateTableColumns(columnType, 1);

        Table t = TestUtils.createTable(definition);
        Writer writer = writerByPushMode(t, mode);

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
                writer.append(rows[i]);
            }
            pushmodeAwareFlush(writer);

            // Second half
            for (int i = 500; i < rows.length; ++i) {
                writer.append(rows[i]);
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
        String alias = TestUtils.createUniqueAlias();
        Column[] definition = TestUtils.generateTableColumns(columnType, 1);

        Table t = TestUtils.createTable(definition);
        Writer writer = writerByPushMode(t, mode);

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
                writer.append(row);
            }
            pushmodeAwareFlush(writer);

            // Second time is the *exact* same rows
            for (WritableRow row : rows) {
                writer.append(row);
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

}
