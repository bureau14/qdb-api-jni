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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.InvalidIteratorException;
import net.quasardb.qdb.exception.InvalidArgumentException;

public class PointsTest {

    private static Session s;

    @BeforeAll
    public static void setup() {
        s = TestUtils.createSession();
    }

    @AfterAll
    public static void teardown() {
        s.close();
        s = null;
    }

    /**
     * Column type provided. This is needed (rather than an @EnumSource(Column.Type.class)), since
     * there also is a 'Column.Type.UNINTIALIZED', which we _do not_ want to test.
     */
    static Stream<Arguments> columnTypeProvider() {
        return Stream.of(Arguments.of(Column.Type.DOUBLE),
                         Arguments.of(Column.Type.INT64),
                         Arguments.of(Column.Type.BLOB),
                         Arguments.of(Column.Type.TIMESTAMP),
                         Arguments.of(Column.Type.STRING),
                         Arguments.of(Column.Type.SYMBOL));
    }

    @ParameterizedTest
    @MethodSource("columnTypeProvider")
    public void canInsert(Column.Type columnType) throws Exception {
        Column column = TestUtils.generateTableColumn(columnType);
        Table table = TestUtils.createTable(s, new Column[]{ column });
        Points values = TestUtils.generatePointsByColumnType(columnType);

        Points.insert(s, table, column, values);
    }

    @ParameterizedTest
    @MethodSource("columnTypeProvider")
    public void canInsertAndRetrieve(Column.Type columnType) throws Exception {
        Column column = TestUtils.generateTableColumn(columnType);
        Table table = TestUtils.createTable(s, new Column[]{ column });
        Points values = TestUtils.generatePointsByColumnType(columnType);

        Points.insert(s, table.getName(), column, values);

        Points ret = Points.get(s,
                                table.getName(),
                                column);

        assertEquals(values, ret);
   }

}
