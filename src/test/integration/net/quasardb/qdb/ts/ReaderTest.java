import java.util.*;
import java.time.*;
import java.lang.Exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.InvalidIteratorException;
import net.quasardb.qdb.exception.InvalidArgumentException;

public class ReaderTest {

    @Test
    public void canGetReader() throws Exception {
        Session s = TestUtils.createSession();

        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Reader reader = Table.reader(s, table, ranges);
    }

    @Test
    public void canCloseReader() throws Exception {
        Session s = TestUtils.createSession();

        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Reader reader = Table.reader(s, table, ranges);
        reader.close();
    }

    @Test
    public void readWithoutRanges_throwsException() throws Exception {
        Session s = TestUtils.createSession();

        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = {};

        assertThrows(InvalidArgumentException.class, () -> {
                Table.reader(s, table, ranges);
            });
    }

    @Test
    public void canReadEmptyResult() throws Exception {
        Session s = TestUtils.createSession();
        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = {};
        Table table = TestUtils.seedTable(s, cols, rows);

        // These ranges should always be empty
        TimeRange[] ranges = {
            new TimeRange(Timespec.now(),
                          Timespec.now().plusNanos(1))
        };

        Reader reader = Table.reader(s, table, ranges);

        assertFalse(reader.hasNext());
    }

    @Test
    public void helpersRowGen_generatesDoubleRows() throws Exception {
        Column[] cols = TestUtils.generateTableColumns(Value.Type.DOUBLE, 1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);

        Arrays.stream(cols)
            .forEach((col) ->
                     assertEquals(col.getType(), Value.Type.DOUBLE));

        Arrays.stream(rows)
            .forEach((row) ->
                     Arrays.stream(row.getValues())
                     .forEach((value) ->
                              assertEquals(value.getType(), Value.Type.DOUBLE)));
    }

    @Test
    public void helpersRowGen_generatesBlobRows() throws Exception {
        Column[] cols = TestUtils.generateTableColumns(Value.Type.BLOB, 1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);

        Arrays.stream(cols)
            .forEach((col) ->
                     assertEquals(col.getType(), Value.Type.BLOB));

        Arrays.stream(rows)
            .forEach((row) ->
                     Arrays.stream(row.getValues())
                     .forEach((value) ->
                              assertEquals(value.getType(), Value.Type.BLOB)));
    }

    @Test
    public void canReadSingleValue_afterWriting() throws Exception {
        Session s = TestUtils.createSession();

        Value.Type[] valueTypes = { Value.Type.INT64,
                                    Value.Type.DOUBLE,
                                    Value.Type.TIMESTAMP,
                                    Value.Type.BLOB,
                                    Value.Type.STRING };

        for (Value.Type valueType : valueTypes) {
            // Generate a 1x1 test dataset
            Column[] cols =
                TestUtils.generateTableColumns(valueType, 1);

            WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
            Table table = TestUtils.seedTable(s, cols, rows);
            TimeRange[] ranges = TestUtils.rangesFromRows(rows);

            Reader reader = Table.reader(s, table, ranges);

            assertTrue(reader.hasNext());

            Row row = reader.next();
            assertEquals(rows[0], row);
        }
    }

    @Test
    public void canReadMultipleValues_afterWriting() throws Exception {
        Session s = TestUtils.createSession();

        Value.Type[] valueTypes = { Value.Type.INT64,
                                    Value.Type.DOUBLE,
                                    Value.Type.TIMESTAMP,
                                    Value.Type.BLOB,
                                    Value.Type.STRING };

        for (Value.Type valueType : valueTypes) {
            // Generate a 2x2 test dataset

            Column[] cols =
                TestUtils.generateTableColumns(valueType, 2);
            WritableRow[] rows = TestUtils.generateTableRows(cols, 2);
            Table table = TestUtils.seedTable(s, cols, rows);
            TimeRange[] ranges = TestUtils.rangesFromRows(rows);

            Reader reader = Table.reader(s, table, ranges);

            int index = 0;
            while (reader.hasNext()) {
                assertEquals(rows[index++], reader.next());
            }
        }
    }

    @Test
    public void canCallHasNext_multipleTimes() throws Exception {
        // Generate a 1x1 test dataset
        Session s = TestUtils.createSession();

        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Reader reader = Table.reader(s, table, ranges);

        assertTrue(reader.hasNext());
        assertTrue(reader.hasNext());
        assertTrue(reader.hasNext());

        reader.next();

        assertFalse(reader.hasNext());
        assertFalse(reader.hasNext());
    }

    @Test
    public void invalidIterator_throwsException() throws Exception {
        // Generate a 1x1 test dataset
        Session s = TestUtils.createSession();

        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        // Seeding complete, actual test below this line

        Reader reader = Table.reader(s, table, ranges);
        reader.next();

        assertThrows(InvalidIteratorException.class, () -> {
                reader.next();
            });
    }
}
