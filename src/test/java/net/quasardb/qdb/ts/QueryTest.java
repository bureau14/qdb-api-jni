import java.util.StringJoiner;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.*;

import net.quasardb.qdb.exception.InputException;

import net.quasardb.common.TestUtils;

public class QueryTest {


    private Session s;

    @BeforeEach
    public void setup() {
        s = TestUtils.createSession();
        s.purgeAll(30000);
    }

    @AfterEach
    public void teardown() {
        s.close();
        s = null;
    }

    @Test
    public void canCreateEmptyQuery() throws Exception {
        Query q = Query.create();
    }

    @Test
    public void canCreateStringyQuery() throws Exception {
        Query q = Query.of("");
    }

    @Test
    public void cannotExecuteEmptyQuery() throws Exception {
        assertThrows(InputException.class, () -> {
                Query.create()
                    .execute(s);
            });
    }

    @Test
    public void canExecuteValidQuery() throws Exception {
        Column.Type[] columnTypes = { Column.Type.INT64,
                                      Column.Type.DOUBLE,
                                      Column.Type.TIMESTAMP,
                                      Column.Type.BLOB,
                                      Column.Type.STRING };

        for (Column.Type columnType : columnTypes) {
            Column[] definition =
                TestUtils.generateTableColumns(columnType, 1);

            WritableRow[] rows = TestUtils.generateTableRows(definition, 1);

            Table t = TestUtils.seedTable(s, definition, rows);

            Result r = new QueryBuilder()
                .add("select")
                .add(definition[0].getName())
                .add("from")
                .add(t.getName())
                .in(TestUtils.rangeFromRows(rows))
                .asQuery()
                .execute(s);

            assertEquals(r.columns.length, definition.length);
            assertEquals(r.rows.length, rows.length);
            assertEquals(r.columns[0], definition[0].getName());

            Row originalRow = rows[0];
            Row outputRow = r.rows[0];

            assertEquals(outputRow, originalRow);
        }
    }

    @Test
    public void canAccessResultAsStream() throws Exception {
        Column.Type[] columnTypes = { Column.Type.INT64,
                                      Column.Type.DOUBLE,
                                      Column.Type.TIMESTAMP,
                                      Column.Type.BLOB,
                                      Column.Type.STRING };

        for (Column.Type columnType : columnTypes) {
            Column[] definition =
                TestUtils.generateTableColumns(columnType, 1);

            WritableRow[] rows = TestUtils.generateTableRows(definition, 10);
            Table t = TestUtils.seedTable(s, definition, rows);

            Result r = new QueryBuilder()
                .add("select")
                .add(definition[0].getName())
                .add("from")
                .add(t.getName())
                .in(TestUtils.rangeFromRows(rows))
                .asQuery()
                .execute(s);

            assertEquals(r.stream().count(), r.rows.length);
        }

    }

    @Test
    public void nullValuesInResultsTest() throws Exception {
        Column.Type[] columnTypes = { Column.Type.INT64,
                                      Column.Type.DOUBLE,
                                      Column.Type.TIMESTAMP,
                                      Column.Type.BLOB,
                                      Column.Type.STRING };

        for (Column.Type columnType : columnTypes) {
            Column[] definition =
                TestUtils.generateTableColumns(columnType, 1);

            WritableRow[] rows = TestUtils.generateTableRows(definition, 32, 10, 0.5);
            Table t = TestUtils.seedTable(s, definition, rows);

            QueryBuilder b = new QueryBuilder()
                .add("select ");

            boolean first = true;
            for (Column c : definition) {
                if (!first) {
                    b = b.add(", ");
                } else {
                    first = false;
                }

                b = b.add(c.getName());
            }

            Result r = b.add(" from ")
                .add(t.getName())
                .in(TestUtils.rangeFromRows(rows))
                .asQuery()
                .execute(s);

            // + 2 because of $timestamp and $table
            assertEquals(definition.length, r.columns.length);
            assertEquals(rows.length, r.rows.length);

            for (int i = 0; i < rows.length; ++i) {
                assertArrayEquals(rows[i].getValues(), r.rows[i].getValues());
            }
        }
    }
}
