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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.kv.*;
import net.quasardb.qdb.ts.Timespec;

public class TimestampTest {

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

    @Test
    public void canCheckExists() throws Exception {
        String k = TestUtils.createUniqueAlias();
        Timespec v = TestUtils.randomTimestamp();
        TimestampEntry d = TimestampEntry.ofAlias(this.s, k);

        assertEquals(false, d.exists());
    }

    @Test
    public void canPut() throws Exception {
        String k = TestUtils.createUniqueAlias();
        Timespec v = TestUtils.randomTimestamp();
        TimestampEntry d = TimestampEntry.ofAlias(this.s, k);

        d.put(v);

        assertEquals(true, d.exists());
    }

    @Test
    public void canGet() throws Exception {
        String k = TestUtils.createUniqueAlias();
        Timespec v = TestUtils.randomTimestamp();
        TimestampEntry d = TimestampEntry.ofAlias(this.s, k);

        d.put(v);

        assertEquals(true, d.exists());
        assertEquals(v, d.get());
    }

    @Test
    public void canUpdate() throws Exception {
        String k = TestUtils.createUniqueAlias();
        Timespec v1 = TestUtils.randomTimestamp();
        TimestampEntry d = TimestampEntry.ofAlias(this.s, k);

        boolean created1 = d.update(v1);
        assertEquals(true, created1);
        assertEquals(v1, d.get());

        Timespec v2 = TestUtils.randomTimestamp();
        boolean created2 = d.update(v2);
        assertEquals(false, created2);

        assertEquals(v2, d.get());
    }

    @Test
    public void canRemove() throws Exception {
        String k = TestUtils.createUniqueAlias();
        Timespec v = TestUtils.randomTimestamp();
        TimestampEntry d = TimestampEntry.ofAlias(this.s, k);

        assertEquals(false, d.exists());

        d.put(v);

        assertEquals(true, d.exists());

        d.remove();

        assertEquals(false, d.exists());
    }

}
