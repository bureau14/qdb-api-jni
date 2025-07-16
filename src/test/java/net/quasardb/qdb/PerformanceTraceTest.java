package net.quasardb.qdb;

import java.io.IOException;
import java.util.Collection;
import java.util.Arrays;

import org.slf4j.event.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.ts.*;

public class PerformanceTraceTest {

    private Session s;

    @BeforeEach
    public void setup() {
        s = TestUtils.createSession();
        s.purgeAll(30000);
        PerformanceTrace.enable(s);
        PerformanceTrace.clear(s);
    }

    @AfterEach
    public void teardown() {
        s.close();
        s = null;
    }

    @Test
    public void canEnablePerformanceTrace() {
        PerformanceTrace.enable(s);
    }


    @Test
    public void canDisablePerformanceTrace() {
        PerformanceTrace.disable(s);
    }


    @Test
    public void canReenablePerformanceTrace() {
        PerformanceTrace.enable(s);
        PerformanceTrace.disable(s);
    }


    @Test
    public void canGetEmptyPerformanceTraces() {
        PerformanceTrace.enable(s);

        Collection<PerformanceTrace.Trace> res = PerformanceTrace.get(s);
        assertEquals(res.size(), 0);
    }

    @Test
    public void canGetTableCreatePerformanceTraces() throws IOException {
        PerformanceTrace.enable(s);

        Column[] columns = TestUtils.generateTableColumns(16);
        Table t = TestUtils.createTable(s, columns);

        Collection<PerformanceTrace.Trace> res = PerformanceTrace.get(s);
        assertEquals(3, res.size());

        for (PerformanceTrace.Trace trace : res) {
            assertTrue(trace.measurements.length > 1);
            assertTrue(trace.name.isEmpty() == false);

            boolean first = true;
            for (PerformanceTrace.Measurement m : trace.measurements) {
                assertTrue(m.label.isEmpty() == false);
                assertEquals(m.elapsed == 0, first);
                first = false;
            }
        }
    }

    @Test
    public void canClearTraces() throws IOException {
        PerformanceTrace.enable(s);

        Column[] columns = TestUtils.generateTableColumns(16);
        Table t = TestUtils.createTable(s, columns);

        Collection<PerformanceTrace.Trace> res1 = PerformanceTrace.get(s);
        PerformanceTrace.clear(s);
        Collection<PerformanceTrace.Trace> res2 = PerformanceTrace.get(s);

        assertEquals(3, res1.size());
        assertEquals(0, res2.size());
    }

    @Test
    public void canPopTraces() throws IOException {
        PerformanceTrace.enable(s);

        Column[] columns = TestUtils.generateTableColumns(16);
        Table t = TestUtils.createTable(s, columns);

        Collection<PerformanceTrace.Trace> res1 = PerformanceTrace.pop(s);
        Collection<PerformanceTrace.Trace> res2 = PerformanceTrace.get(s);

        assertEquals(3, res1.size());
        assertEquals(0, res2.size());
    }

    @Test
    public void canCollectBatchPushTraces() throws IOException {
        PerformanceTrace.enable(s);

        Column[] columns = TestUtils.generateTableColumns(16);

        Collection<PerformanceTrace.Trace> res1 = PerformanceTrace.pop(s);

        Table t = TestUtils.createTable(s, columns);

        Collection<PerformanceTrace.Trace> res2 = PerformanceTrace.pop(s);

        System.out.println("res2: " + res2);

        assertEquals(0, res1.size());
        assertEquals(3, res2.size());

        Writer w = Writer.builder(s).build();

        Collection<PerformanceTrace.Trace> res3 = PerformanceTrace.pop(s);
        assertEquals(0, res3.size());

        WritableRow[] rows = TestUtils.generateTableRows(columns, 32);
        for (WritableRow row : rows) {
            w.append(t, row);
        }

        Collection<PerformanceTrace.Trace> res4 = PerformanceTrace.pop(s);
        assertEquals(0, res4.size());

        w.flush();

        Collection<PerformanceTrace.Trace> res5 = PerformanceTrace.pop(s);\
        System.out.println("res5: " + res5);
        assertEquals(2, res5.size());
    }

    @Test
    public void canLogTraces() throws IOException {
        PerformanceTrace.enable(s);

        Column[] columns = TestUtils.generateTableColumns(16);

        Collection<PerformanceTrace.Trace> res1 = PerformanceTrace.pop(s);

        Table t = TestUtils.createTable(s, columns);

        PerformanceTrace.log(s);
    }
}
