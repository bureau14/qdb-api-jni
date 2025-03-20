package net.quasardb.qdb;

import java.io.IOException;
import java.util.Collection;
import java.util.Arrays;

import org.slf4j.event.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.exception.InputBufferTooSmallException;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;

public class SessionOptionsTest {

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
    public void canGetInputBufferSize() {
        assertTrue(this.s.getInputBufferSize() > 1);
    }


    @Test
    public void canSetInputBufferSize() {
        long old = this.s.getInputBufferSize();
        this.s.setInputBufferSize(old + 1024);

        assertEquals(this.s.getInputBufferSize(), old + 1024);
    }

    @Test
    public void canGetConnectionPerAddressSoftLimit() {
        assertTrue(this.s.getConnectionPerAddressSoftLimit() > 0);
    }

    @Test
    public void canSetConnectionPerAddressSoftLimit() {
        long old = this.s.getConnectionPerAddressSoftLimit();
        this.s.setConnectionPerAddressSoftLimit(old + 256);
        assertEquals(this.s.getConnectionPerAddressSoftLimit(), old + 256);
    }

    @Test
    public void canGetMaxBatchLoad() {
        assertTrue(this.s.getMaxBatchLoad() > 0);
    }

    @Test
    public void canSetMaxBatchLoad() {
        long old = this.s.getMaxBatchLoad();
        this.s.setMaxBatchLoad(old + 42);
        assertEquals(this.s.getMaxBatchLoad(), old + 42);
    }

    @Test
    public void canGetClientMaxParallelism() {
        assertTrue(this.s.getClientMaxParallelism() > 0);
    }

    @Test
    public void canSetSoftMemoryLimit() {
        // 4GiB
        long limit = 4294967296L;
        this.s.setSoftMemoryLimit(limit);
    }

    @Test
    public void canGetMemoryInfo() {
        String info1 = this.s.getMemoryInfo();
        assertTrue(info1.contains("TBB huge threshold bytes"));

        long limit = 2147483648L;
        this.s.setSoftMemoryLimit(limit);
        String info2 = this.s.getMemoryInfo();

        assertTrue(info2.contains("TBB soft limit bytes = 2147483648"));
    }

    @Test
    public void canTidyMemory() {
        this.s.tidyMemory();
    }

    @Test
    public void throwsExceptionOnSmallBuffer() throws Exception {
        // Create a table with 100x10 blobs, set a ridiculously low input
        // buffer size, and retrieve all data to force a buffer size issue
        // to pop up.

        this.s.setInputBufferSize(1500);

        Column[] definition =
            TestUtils.generateTableColumns(Column.Type.BLOB, 10);

        WritableRow[] rows = TestUtils.generateTableRows(definition, 100);
        Table t = TestUtils.seedTable(this.s, definition, rows);

        Query r = new QueryBuilder()
            .add("select * from " + t.getName())
            .asQuery();

        assertThrows(InputBufferTooSmallException.class, () -> {
                r.execute(this.s);
            });
    }
}
