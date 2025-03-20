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

public class BlobTest {

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
        ByteBuffer v = TestUtils.randomBlob();
        BlobEntry b = BlobEntry.ofAlias(this.s, k);

        assertEquals(false, b.exists());
    }

    @Test
    public void canPut() throws Exception {
        String k = TestUtils.createUniqueAlias();
        ByteBuffer v = TestUtils.randomBlob();
        BlobEntry b = BlobEntry.ofAlias(this.s, k);

        b.put(v);

        assertEquals(true, b.exists());
    }

    @Test
    public void canGet() throws Exception {
        String k = TestUtils.createUniqueAlias();
        ByteBuffer bb = TestUtils.randomBlob();
        BlobEntry b = BlobEntry.ofAlias(this.s, k);

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
        BlobEntry b = BlobEntry.ofAlias(this.s, k);

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
        BlobEntry b = BlobEntry.ofAlias(this.s, k);

        assertEquals(false, b.exists());

        b.put(bb);

        assertEquals(true, b.exists());

        b.remove();

        assertEquals(false, b.exists());
    }

}
