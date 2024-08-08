package net.quasardb.qdb;

import java.io.IOException;
import java.util.Collection;
import java.util.Arrays;

import org.slf4j.event.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.exception.InputBufferTooSmallException;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;

public class SessionOptionsTest {

    @Test
    public void canGetInputBufferSize() {
        Session s = TestUtils.createSession();
        assertTrue(s.getInputBufferSize() > 1);
    }


    @Test
    public void canSetInputBufferSize() {
        Session s = TestUtils.createSession();

        long old = s.getInputBufferSize();
        s.setInputBufferSize(old + 1024);

        assertEquals(s.getInputBufferSize(), old + 1024);
    }


    @Test
    public void canGetClientMaxParallelism() {
        Session s = TestUtils.createSession();
        assertTrue(s.getClientMaxParallelism() > 0);
    }

    @Test
    public void canSetSoftMemoryLimit() {
        Session s = TestUtils.createSession();

        // 4GiB
        long limit = 4294967296L;
        s.setSoftMemoryLimit(limit);
    }

    @Test
    public void canGetMemoryInfo() {
        Session s = TestUtils.createSession();

        String info1 = s.getMemoryInfo();
        assertTrue(info1.contains("TBB huge threshold bytes"));

        long limit = 2147483648L;
        s.setSoftMemoryLimit(limit);
        String info2 = s.getMemoryInfo();

        assertTrue(info2.contains("TBB soft limit bytes = 2147483648"));
    }

    @Test
    public void canTidyMemory() {
        Session s = TestUtils.createSession();

        s.tidyMemory();
    }

    @Test
    public void throwsExceptionOnSmallBuffer() throws Exception {
        // Create a table with 100x10 blobs, set a ridiculously low input
        // buffer size, and retrieve all data to force a buffer size issue
        // to pop up.

        Session s = TestUtils.createSession();
        s.setInputBufferSize(1500);

        Column[] definition =
            TestUtils.generateTableColumns(Column.Type.BLOB, 10);

        WritableRow[] rows = TestUtils.generateTableRows(definition, 100);
        Table t = TestUtils.seedTable(s, definition, rows);

        Query r = new QueryBuilder()
            .add("select * from " + t.getName())
            .asQuery();

        assertThrows(InputBufferTooSmallException.class, () -> {
                r.execute(s);
            });
    }
}
