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

public class BlobTest {

    private Session s;

    @BeforeEach
    public void setup() {
        s = TestUtils.createSession();
    }

    @AfterEach
    public void teardown() {
        s.close();
        s = null;
    }

    @Test
    public void canCheckExists() throws Exception {
        String k = TestUtils.createUniqueAlias();
        ByteBuffer v = TestUtils.randomBlob();
        BlobEntry b = BlobEntry.ofAlias(s, k);

        assertEquals(false, b.exists());
    }

    @Test
    public void canPut() throws Exception {
        String k = TestUtils.createUniqueAlias();
        ByteBuffer v = TestUtils.randomBlob();
        BlobEntry b = BlobEntry.ofAlias(s, k);

        b.put(v);

        assertEquals(true, b.exists());
    }

    @Test
    public void canGet() throws Exception {
        String k = TestUtils.createUniqueAlias();
        ByteBuffer bb = TestUtils.randomBlob();
        BlobEntry b = BlobEntry.ofAlias(s, k);

        b.put(bb);

        assertEquals(true, b.exists());

        Buffer v_ = b.get();
        ByteBuffer bb_ = v_.toByteBuffer();

        assertEquals(bb, bb_);
    }

    @Test
    public void canUpdate() throws Exception {
        String k = TestUtils.createUniqueAlias();
        ByteBuffer bb1 = TestUtils.randomBlob();
        BlobEntry b = BlobEntry.ofAlias(s, k);

        boolean created1 = b.update(bb1);
        assertEquals(true, created1);

        assertEquals(bb1, b.get().toByteBuffer());

        ByteBuffer bb2 = TestUtils.randomBlob();
        boolean created2 = b.update(bb2);
        assertEquals(false, created2);

        assertEquals(bb2, b.get().toByteBuffer());
    }

    @Test
    public void canRemove() throws Exception {
        String k = TestUtils.createUniqueAlias();
        ByteBuffer bb = TestUtils.randomBlob();
        BlobEntry b = BlobEntry.ofAlias(s, k);

        assertEquals(false, b.exists());

        b.put(bb);

        assertEquals(true, b.exists());

        b.remove();

        assertEquals(false, b.exists());
    }

}
