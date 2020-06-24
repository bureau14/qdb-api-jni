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
    public void throwsExceptionOnSmallBuffer() throws Exception {
        // Create a table with 100x10 blobs, set a ridiculously low input
        // buffer size, and retrieve all data to force a buffer size issue
        // to pop up.

        Session s = TestUtils.createSession();
        s.setInputBufferSize(1024);

        Column[] definition =
            TestUtils.generateTableColumns(Value.Type.BLOB, 10);

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
