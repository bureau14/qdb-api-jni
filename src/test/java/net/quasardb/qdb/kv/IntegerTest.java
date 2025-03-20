import java.util.*;
import java.time.*;
import java.lang.Exception;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.kv.*;

public class IntegerTest {

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

    @Test
    public void canCheckExists() throws Exception {
        String k = TestUtils.createUniqueAlias();
        long v = TestUtils.randomInt64();
        IntegerEntry i = IntegerEntry.ofAlias(s, k);

        assertEquals(false, i.exists());
    }

    @Test
    public void canPut() throws Exception {
        String k = TestUtils.createUniqueAlias();
        long v = TestUtils.randomInt64();
        IntegerEntry i = IntegerEntry.ofAlias(s, k);

        i.put(v);

        assertEquals(true, i.exists());
    }

    @Test
    public void canGet() throws Exception {
        String k = TestUtils.createUniqueAlias();
        long v = TestUtils.randomInt64();
        IntegerEntry i = IntegerEntry.ofAlias(s, k);

        i.put(v);

        assertEquals(true, i.exists());
        assertEquals(v, i.get());
    }

    @Test
    public void canUpdate() throws Exception {
        String k = TestUtils.createUniqueAlias();
        long v1 = TestUtils.randomInt64();
        IntegerEntry i = IntegerEntry.ofAlias(s, k);

        boolean created1 = i.update(v1);
        assertEquals(true, created1);
        assertEquals(v1, i.get());

        long v2 = TestUtils.randomInt64();
        boolean created2 = i.update(v2);
        assertEquals(false, created2);

        assertEquals(v2, i.get());
    }

    @Test
    public void canRemove() throws Exception {
        String k = TestUtils.createUniqueAlias();
        long v = TestUtils.randomInt64();
        IntegerEntry i = IntegerEntry.ofAlias(s, k);

        assertEquals(false, i.exists());

        i.put(v);

        assertEquals(true, i.exists());

        i.remove();

        assertEquals(false, i.exists());
    }

}
