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
import net.quasardb.qdb.kv.StringEntry;
import net.quasardb.qdb.kv.IntegerEntry;
import net.quasardb.qdb.kv.DoubleEntry;

public class BatchTest {

    private Session s;

    @BeforeEach
    public void setup() {
        this.s = TestUtils.createSession();
    }

    @AfterEach
    public void teardown() {
        // this.s.purgeAll(300000);
        this.s.close();
        this.s = null;
    }

    // @Test
    // public void canCreateBatch() throws Exception {
    //     Batch b = Batch.builder(this.s).build();
    // }

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

    @ParameterizedTest
    @EnumSource(Batch.CommitMode.class)
    public void canBatchStrings(Batch.CommitMode commitMode) throws Exception {
        // Random key/value
        String k = TestUtils.createUniqueAlias();
        String v1 = TestUtils.randomString();
        String v2 = TestUtils.randomString();

        Batch b = Batch.builder(this.s).commitMode(commitMode).build();

        // First put new string
        b.string(k).put(v1);

        // Then update to v2
        b.string(k).update(v2);
        assertEquals(b.size(), 2);

        // Commit the batch, ensure it's now empty
        b.commit();

        // Validate entry actually exists using regular key/value API
        StringEntry s_ = StringEntry.ofAlias(this.s, k);
        // assertEquals(true, s_.exists());

        String v_ = s_.get();

        switch (commitMode) {
        case FAST:
            // Fast mode only does the first update
            assertEquals(v1, v_);
            break;

        case TRANSACTIONAL:
            // All operations are executed in order
            assertEquals(v2, v_);
            break;
        }
    }


    @ParameterizedTest
    @EnumSource(Batch.CommitMode.class)
    public void canBatchIntegers(Batch.CommitMode commitMode) throws Exception {
        // Random key/value
        String k = TestUtils.createUniqueAlias();
        long v1 = TestUtils.randomInt64();
        long v2 = TestUtils.randomInt64();

        Batch b = Batch.builder(this.s).commitMode(commitMode).build();

        // First put new string
        b.integer(k).put(v1);

        // Then update to v2
        b.integer(k).update(v2);
        assertEquals(b.size(), 2);

        // Commit the batch, ensure it's now empty
        b.commit();

        // Validate entry actually exists using regular key/value API
        IntegerEntry i_ = IntegerEntry.ofAlias(this.s, k);
        assertEquals(true, i_.exists());

        long v_ = i_.get();

        switch (commitMode) {
        case FAST:
            // Fast mode only does the first update
            assertEquals(v1, v_);
            break;

        case TRANSACTIONAL:
            // All operations are executed in order
            assertEquals(v2, v_);
            break;
        }
    }

    @ParameterizedTest
    @EnumSource(Batch.CommitMode.class)
    public void canBatchDoubles(Batch.CommitMode commitMode) throws Exception {
        // Random key/value
        String k = TestUtils.createUniqueAlias();
        double v1 = TestUtils.randomDouble();
        double v2 = TestUtils.randomDouble();

        Batch b = Batch.builder(this.s).commitMode(commitMode).build();

        // First put new string
        b.double_(k).put(v1);

        // Then update to v2
        b.double_(k).update(v2);
        assertEquals(b.size(), 2);

        // Commit the batch, ensure it's now empty
        b.commit();

        // Validate entry actually exists using regular key/value API
        DoubleEntry d_ = DoubleEntry.ofAlias(this.s, k);
        assertEquals(true, d_.exists());

        double v_ = d_.get();

        switch (commitMode) {
        case FAST:
            // Fast mode only does the first update
            assertEquals(v1, v_);
            break;

        case TRANSACTIONAL:
            // All operations are executed in order
            assertEquals(v2, v_);
            break;
        }
    }


}
