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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.*;

import net.quasardb.qdb.exception.InvalidArgumentException;
import net.quasardb.qdb.exception.InputException;

import net.quasardb.common.TestUtils;

public class TableTest {

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
    public void canCreateEmptyTable() throws Exception {
        Column[] columns = TestUtils.generateTableColumns(8);
        Table.create(s, TestUtils.createUniqueAlias(), columns);
    }

    @ParameterizedTest
    @ValueSource(longs = {3000, 60000, 180000, 86400000})
    public void canCreateTableWithShardSize(long shardSize) throws Exception {
        Column[] columns = TestUtils.generateTableColumns(8);
        Table.create(s, TestUtils.createUniqueAlias(), columns, shardSize);
    }

    @ParameterizedTest
    @ValueSource(longs = {3000, 60000, 180000, 86400000})
    public void canQueryShardSize(long shardSize) throws Exception {
        Column[] columns = TestUtils.generateTableColumns(8);
        Table t = Table.create(s, TestUtils.createUniqueAlias(), columns, shardSize);
        assertEquals(Table.getShardSize(s, t), shardSize / 1000);
        assertEquals(Table.getShardSizeMillis(s, t), shardSize);

        assertEquals(t.getShardSize(), shardSize / 1000);
        assertEquals(t.getShardSizeMillis(), shardSize);
    }

    @Test
    public void createAddsTimestampColumnWhenMissing() throws Exception {
        Column[] columns = new Column[] {
            new Column.Double("value"),
            new Column.String_("label")
        };

        Table t = Table.create(s, TestUtils.createUniqueAlias(), columns);

        Column[] actual = t.getColumns();
        assertEquals(2, actual.length);
        assertEquals("value", actual[0].getName());
        assertEquals(Column.Type.DOUBLE, actual[0].getType());
        assertEquals("label", actual[1].getName());
        assertEquals(Column.Type.STRING, actual[1].getType());
    }

    @Test
    public void createKeepsExplicitTimestampColumnFirst() throws Exception {
        Column[] columns = new Column[] {
            new Column.Timestamp("$timestamp"),
            new Column.Double("value")
        };

        Table t = Table.create(s, TestUtils.createUniqueAlias(), columns);

        Column[] actual = t.getColumns();
        assertEquals(1, actual.length);
        assertEquals("value", actual[0].getName());
        assertEquals(Column.Type.DOUBLE, actual[0].getType());
    }

    @Test
    public void createEmptySchemaRejectsTableCreation() {
        assertThrows(InvalidArgumentException.class,
            () -> Table.create(s, TestUtils.createUniqueAlias(), new Column[0]));
    }

    @Test
    public void createRejectsTimestampColumnWhenNotFirst() {
        Column[] columns = new Column[] {
            new Column.Double("value"),
            new Column.Timestamp("$timestamp")
        };

        assertThrows(InvalidArgumentException.class,
            () -> Table.create(s, TestUtils.createUniqueAlias(), columns));
    }

    @Test
    public void createRejectsTimestampColumnWithWrongType() {
        Column[] columns = new Column[] {
            new Column.String_("$timestamp"),
            new Column.Double("value")
        };

        assertThrows(InvalidArgumentException.class,
            () -> Table.create(s, TestUtils.createUniqueAlias(), columns));
    }
}
