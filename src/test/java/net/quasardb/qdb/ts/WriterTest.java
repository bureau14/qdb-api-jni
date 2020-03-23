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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.api.Test;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.InvalidIteratorException;
import net.quasardb.qdb.exception.InvalidArgumentException;

public class WriterTest {

    static Stream<Arguments> pushModeAndValueTypeProvider() {
        return Stream.of(Arguments.of(Writer.PushMode.NORMAL, Value.Type.DOUBLE),
                         Arguments.of(Writer.PushMode.NORMAL, Value.Type.INT64),
                         Arguments.of(Writer.PushMode.NORMAL, Value.Type.BLOB),
                         Arguments.of(Writer.PushMode.NORMAL, Value.Type.TIMESTAMP),

                         Arguments.of(Writer.PushMode.ASYNC, Value.Type.DOUBLE),
                         Arguments.of(Writer.PushMode.ASYNC, Value.Type.INT64),
                         Arguments.of(Writer.PushMode.ASYNC, Value.Type.BLOB),
                         Arguments.of(Writer.PushMode.ASYNC, Value.Type.TIMESTAMP),

                         Arguments.of(Writer.PushMode.FAST, Value.Type.DOUBLE),
                         Arguments.of(Writer.PushMode.FAST, Value.Type.INT64),
                         Arguments.of(Writer.PushMode.FAST, Value.Type.BLOB),
                         Arguments.of(Writer.PushMode.FAST, Value.Type.TIMESTAMP)

                         );
    }




    Writer writerByPushMode(Session s, Table t, Writer.PushMode mode) {
        switch (mode) {
        case NORMAL:
            return t.writer(s, t);
        case FAST:
            return t.fastWriter(s, t);
        case ASYNC:
            return t.asyncWriter(s, t);
        }

        throw new IllegalArgumentException("Invalid push mode: " + mode.toString());
    }

    Writer writerByPushMode(Session s, Tables t, Writer.PushMode mode) {
        switch (mode) {
        case NORMAL:
            return t.writer(s, t);
        case FAST:
            return t.fastWriter(s, t);
        case ASYNC:
            return t.asyncWriter(s, t);
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
        Session s = TestUtils.createSession();

        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Writer w = writerByPushMode(s, table, mode);
    }


    @ParameterizedTest
    @EnumSource(Writer.PushMode.class)
    public void canFlushWriter(Writer.PushMode mode) throws Exception {
        Session s = TestUtils.createSession();

        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Writer w = writerByPushMode(s, table, mode);
        w.flush();
    }

    @ParameterizedTest
    @EnumSource(Writer.PushMode.class)
    public void canCloseWriter(Writer.PushMode mode) throws Exception {
        Session s = TestUtils.createSession();

        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Writer w = writerByPushMode(s, table, mode);
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

        Session s = TestUtils.createSession();
        Table table1 = TestUtils.createTable(columns);
        Table table2 = TestUtils.createTable(columns);

        Tables tables = new Tables(new Table[] {table1, table2});

        Writer writer = writerByPushMode(s, tables, mode);

        assertEquals(writer.tableIndexByName(table1.getName()), 0);
        assertEquals(writer.tableIndexByName(table2.getName()), columns.length);
    }

    @ParameterizedTest
    @MethodSource("pushModeAndValueTypeProvider")
    public void canInsertRow(Writer.PushMode mode, Value.Type valueType) throws Exception {
        String alias = TestUtils.createUniqueAlias();
        Column[] definition = TestUtils.generateTableColumns(valueType, 1);

        Session s = TestUtils.createSession();
        Table t = TestUtils.createTable(definition);
        Writer writer = writerByPushMode(s, t, mode);

        Value[] values = {
            TestUtils.generateRandomValueByType(valueType)
        };

        Timespec timestamp = new Timespec(LocalDateTime.now());
        WritableRow writeRow = new WritableRow(timestamp, values);
        writer.append(writeRow);

        pushmodeAwareFlush(writer);

        TimeRange[] ranges = {
            new TimeRange(timestamp,
                          timestamp.plusNanos(1))
        };

        Reader r = Table.reader(s, t, ranges);

        assertTrue(r.hasNext());
        Row readRow = r.next();

        assertEquals(readRow, writeRow);
    }


    @ParameterizedTest
    @MethodSource("pushModeAndValueTypeProvider")
    public void canInsertMultipleRows(Writer.PushMode mode, Value.Type valueType) throws Exception {
        String alias = TestUtils.createUniqueAlias();
        Column[] definition = TestUtils.generateTableColumns(valueType, 1);

        Session s = TestUtils.createSession();
        Table t = TestUtils.createTable(definition);
        Writer writer = writerByPushMode(s, t, mode);

        int ROW_COUNT = 1000;

        WritableRow[] rows = new WritableRow[ROW_COUNT];
        for (int i = 0; i < rows.length; ++i) {
            rows[i] =
                new WritableRow (LocalDateTime.now(),
                                 new Value[] {
                                     TestUtils.generateRandomValueByType(valueType)});
            writer.append(rows[i]);
        }

        pushmodeAwareFlush(writer);


        TimeRange[] ranges = {
            new TimeRange(rows[0].getTimestamp(),
                          new Timespec(rows[(rows.length - 1)].getTimestamp().asLocalDateTime().plusNanos(1)))
        };

        WritableRow[] readRows = Table.reader(s, t, ranges).stream().toArray(WritableRow[]::new);

        assertArrayEquals(rows, readRows);
    }
}
