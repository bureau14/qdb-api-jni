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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.batch.Batch;
import net.quasardb.qdb.kv.BlobEntry;

public class BatchTest {

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
    public void canCreateBatch() throws Exception {
        Batch b = Batch.builder(this.s).build();
    }


    @Test
    public void canPutBlob() throws Exception {
        // Random key/value
        String k = TestUtils.createUniqueAlias();
        ByteBuffer bb = TestUtils.randomBlob();

        // Create batch, put blob in batch
        Batch b = Batch.builder(this.s).build();
        assertTrue(b.isEmpty());

        b.blob(k).put(bb);
        assertFalse(b.isEmpty());
        assertEquals(b.size(), 1);

        // Commit the batch, ensure it's now empty
        b.commit();
        assertTrue(b.isEmpty());

        // Validate entry actually exists using regular key/value API
        BlobEntry b_ = BlobEntry.ofAlias(this.s, k);
        assertEquals(true, b_.exists());

        Buffer v_ = b_.get();
        ByteBuffer bb_ = v_.toByteBuffer();

        assertEquals(bb, bb_);
    }


    @ParameterizedTest
    @EnumSource(Batch.CommitMode.class)
    public void canBatchBlobs(Batch.CommitMode commitMode) throws Exception {
        // Random key/value
        String k = TestUtils.createUniqueAlias();
        ByteBuffer bb1 = TestUtils.randomBlob();
        ByteBuffer bb2 = TestUtils.randomBlob();

        Batch b = Batch.builder(this.s).commitMode(commitMode).build();

        // First put new blob
        b.blob(k).put(bb1);

        // Then update to bb2
        b.blob(k).update(bb2);
        assertEquals(b.size(), 2);

        // Commit the batch, ensure it's now empty
        b.commit();

        // Validate entry actually exists using regular key/value API
        BlobEntry b_ = BlobEntry.ofAlias(this.s, k);
        assertEquals(true, b_.exists());

        Buffer v_ = b_.get();
        ByteBuffer bb_ = v_.toByteBuffer();

        switch (commitMode) {
        case FAST:
            // Fast mode only does the first update
            assertEquals(bb1, bb_);
            break;

        case TRANSACTIONAL:
            // All operations are executed in order
            assertEquals(bb2, bb_);
            break;
        }
    }




}
