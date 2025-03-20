import java.util.*;
import java.time.*;
import java.lang.Exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.InvalidIteratorException;
import net.quasardb.qdb.exception.InvalidArgumentException;

public class ReaderTest {


    private Session s;

    @BeforeEach
    public void setup() {
        this.s = TestUtils.createSession();
    }

    @AfterEach
    public void teardown() {
        this.s.close();
        this.s = null;
    }

    @Test
    public void canGetReader() throws Exception {
        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(this.s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Reader reader = Table.reader(this.s, table, ranges);
        reader.close();
    }

    @Test
    public void canCloseReader() throws Exception {
        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(this.s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Reader reader = Table.reader(this.s, table, ranges);
        reader.close();
    }

    @Test
    public void readWithoutRanges_throwsException() throws Exception {
        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(this.s, cols, rows);
        TimeRange[] ranges = {};

        assertThrows(InvalidArgumentException.class, () -> {
                Table.reader(this.s, table, ranges);
            });
    }

    @Test
    public void canReadEmptyResult() throws Exception {
        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = {};
        Table table = TestUtils.seedTable(this.s, cols, rows);

        // These ranges should always be empty
        TimeRange[] ranges = {
            new TimeRange(Timespec.now(),
                          Timespec.now().plusNanos(1))
        };

        Reader reader = Table.reader(this.s, table, ranges);
        try {
            assertFalse(reader.hasNext());
        } finally {
            reader.close();
        }
    }

    @Test
    public void helpersRowGen_generatesDoubleRows() throws Exception {
        Column[] cols = TestUtils.generateTableColumns(Column.Type.DOUBLE, 1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);

        Arrays.stream(cols)
            .forEach((col) ->
                     assertEquals(col.getType(), Column.Type.DOUBLE));

        Arrays.stream(rows)
            .forEach((row) ->
                     Arrays.stream(row.getValues())
                     .forEach((value) ->
                              assertEquals(value.getType(), Value.Type.DOUBLE)));
    }

    @Test
    public void helpersRowGen_generatesBlobRows() throws Exception {
        Column[] cols = TestUtils.generateTableColumns(Column.Type.BLOB, 1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);

        Arrays.stream(cols)
            .forEach((col) ->
                     assertEquals(col.getType(), Column.Type.BLOB));

        Arrays.stream(rows)
            .forEach((row) ->
                     Arrays.stream(row.getValues())
                     .forEach((value) ->
                              assertEquals(value.getType(), Value.Type.BLOB)));
    }

    @Test
    public void canReadSingleValue_afterWriting() throws Exception {
        Column.Type[] columnTypes = { Column.Type.INT64,
                                      Column.Type.DOUBLE,
                                      Column.Type.TIMESTAMP,
                                      Column.Type.BLOB,
                                      Column.Type.STRING };

        for (Column.Type columnType : columnTypes) {
            // Generate a 1x1 test dataset
            Column[] cols =
                TestUtils.generateTableColumns(columnType, 1);

            WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
            Table table = TestUtils.seedTable(this.s, cols, rows);
            TimeRange[] ranges = TestUtils.rangesFromRows(rows);

            Reader reader = Table.reader(this.s, table, ranges);

            try {
                assertTrue(reader.hasNext());

                Row row = reader.next();
                assertEquals(rows[0], row);
            } finally {
                reader.close();
            }
        }
    }

    @Test
    public void canReadMultipleValues_afterWriting() throws Exception {
        Column.Type[] columnTypes = { Column.Type.INT64,
                                      Column.Type.DOUBLE,
                                      Column.Type.TIMESTAMP,
                                      Column.Type.BLOB,
                                      Column.Type.STRING };

        for (Column.Type columnType : columnTypes) {
            // Generate a 2x2 test dataset
            Column[] cols =
                TestUtils.generateTableColumns(columnType, 2);
            WritableRow[] rows = TestUtils.generateTableRows(cols, 2);
            Table table = TestUtils.seedTable(this.s, cols, rows);
            TimeRange[] ranges = TestUtils.rangesFromRows(rows);

            Reader reader = Table.reader(this.s, table, ranges);

            try {
                int index = 0;
                while (reader.hasNext()) {
                    assertEquals(rows[index++], reader.next());
                }
            } finally {
                reader.close();
            }
        }
    }

    @Test
    public void canCallHasNext_multipleTimes() throws Exception {
        // Generate a 1x1 test dataset
        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(this.s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        Reader reader = Table.reader(this.s, table, ranges);

        try {
            assertTrue(reader.hasNext());
            assertTrue(reader.hasNext());
            assertTrue(reader.hasNext());

            reader.next();

            assertFalse(reader.hasNext());
            assertFalse(reader.hasNext());
        } finally {
            reader.close();
        }
    }

    @Test
    public void invalidIterator_throwsException() throws Exception {
        // Generate a 1x1 test dataset
        Column[] cols = TestUtils.generateTableColumns(1);
        WritableRow[] rows = TestUtils.generateTableRows(cols, 1);
        Table table = TestUtils.seedTable(this.s, cols, rows);
        TimeRange[] ranges = TestUtils.rangesFromRows(rows);

        // Seeding complete, actual test below this line

        Reader reader = Table.reader(this.s, table, ranges);
        try {
            reader.next();

            assertThrows(InvalidIteratorException.class, () -> {
                    reader.next();
                });
        } finally {
            reader.close();
        }
    }
}
