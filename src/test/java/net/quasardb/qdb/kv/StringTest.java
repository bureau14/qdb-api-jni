import java.util.*;
import java.time.*;
import java.lang.Exception;

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

public class StringTest {

    private Session s;

    @BeforeAll
    public void setup() {
        this.s = TestUtils.createSession();
    }

    @AfterAll
    public void teardown() {
        this.s.close();
        this.s = null;
    }

    @Test
    public void canCheckExists() throws Exception {
        String k = TestUtils.createUniqueAlias();
        String v = TestUtils.randomString();
        StringEntry b = StringEntry.ofAlias(this.s, k);

        assertEquals(false, b.exists());
    }

    @Test
    public void canPut() throws Exception {
        String k = TestUtils.createUniqueAlias();
        String v = TestUtils.randomString();
        StringEntry b = StringEntry.ofAlias(this.s, k);

        b.put(v);

        assertEquals(true, b.exists());
    }

    @Test
    public void canGet() throws Exception {
        String k = TestUtils.createUniqueAlias();
        String v = TestUtils.randomString();
        StringEntry b = StringEntry.ofAlias(this.s, k);

        b.put(v);

        assertEquals(true, b.exists());

        String v_ = b.get();

        assertEquals(v, v_);
    }

    @Test
    public void canUpdate() throws Exception {
        String k = TestUtils.createUniqueAlias();
        String v1 = TestUtils.randomString();
        StringEntry b = StringEntry.ofAlias(this.s, k);

        boolean created1 = b.update(v1);
        assertEquals(true, created1);

        assertEquals(v1, b.get().toString());

        String v2 = TestUtils.randomString();
        boolean created2 = b.update(v2);
        assertEquals(false, created2);

        assertEquals(v2, b.get().toString());
    }

    @Test
    public void canRemove() throws Exception {
        String k = TestUtils.createUniqueAlias();
        String v = TestUtils.randomString();
        StringEntry b = StringEntry.ofAlias(this.s, k);

        assertEquals(false, b.exists());

        b.put(v);

        assertEquals(true, b.exists());

        b.remove();

        assertEquals(false, b.exists());
    }

}
