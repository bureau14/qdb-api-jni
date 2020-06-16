import java.util.StringJoiner;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.*;

import net.quasardb.qdb.exception.InputException;

import net.quasardb.common.TestUtils;

public class TableTest {

    @Test
    public void canCreateEmptyTable() throws Exception {
        Column[] columns = TestUtils.generateTableColumns(8);
        Session s = TestUtils.createSession();
        Table.create(s, TestUtils.createUniqueAlias(), columns);
    }

    @ParameterizedTest
    @ValueSource(longs = {3000, 60000, 180000, 86400000})
    public void canCreateTableWithShardSize(long shardSize) throws Exception {
        Column[] columns = TestUtils.generateTableColumns(8);
        Session s = TestUtils.createSession();
        Table.create(s, TestUtils.createUniqueAlias(), columns, shardSize);
    }

    @ParameterizedTest
    @ValueSource(longs = {3000, 60000, 180000, 86400000})
    public void canQueryShardSize(long shardSize) throws Exception {
        Column[] columns = TestUtils.generateTableColumns(8);
        Session s = TestUtils.createSession();
        Table t = Table.create(s, TestUtils.createUniqueAlias(), columns, shardSize);
        assertEquals(Table.getShardSize(s, t), shardSize / 1000);
        assertEquals(Table.getShardSizeMillis(s, t), shardSize);

        assertEquals(t.getShardSize(), shardSize / 1000);
        assertEquals(t.getShardSizeMillis(), shardSize);
    }
}
