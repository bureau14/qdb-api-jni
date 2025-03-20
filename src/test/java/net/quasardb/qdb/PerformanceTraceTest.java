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
        this.s = TestUtils.createSession();
    }

    @AfterEach
    public void teardown() {
        this.s.purgeAll(300000);
        this.s.close();
        this.s = null;
    }

    @Test
    public void canEnablePerformanceTrace() {
        PerformanceTrace.enable(this.s);
    }


    @Test
    public void canDisablePerformanceTrace() {
        PerformanceTrace.disable(this.s);
    }


    @Test
    public void canReenablePerformanceTrace() {
        PerformanceTrace.enable(this.s);
        PerformanceTrace.disable(this.s);
    }


    @Test
    public void canGetEmptyPerformanceTraces() {
        PerformanceTrace.enable(this.s);

        Collection<PerformanceTrace.Trace> res = PerformanceTrace.get(this.s);
        assertEquals(res.size(), 0);
    }

    @Test
    public void canGetTableCreatePerformanceTraces() throws IOException {
        PerformanceTrace.enable(this.s);

        Column[] columns = TestUtils.generateTableColumns(16);
        Table t = TestUtils.createTable(this.s, columns);

        Collection<PerformanceTrace.Trace> res = PerformanceTrace.get(this.s);
        assertEquals(2, res.size());

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
        PerformanceTrace.enable(this.s);

        Column[] columns = TestUtils.generateTableColumns(16);
        Table t = TestUtils.createTable(this.s, columns);

        Collection<PerformanceTrace.Trace> res1 = PerformanceTrace.get(this.s);
        PerformanceTrace.clear(this.s);
        Collection<PerformanceTrace.Trace> res2 = PerformanceTrace.get(this.s);

        assertEquals(2, res1.size());
        assertEquals(0, res2.size());
    }

    @Test
    public void canPopTraces() throws IOException {
        PerformanceTrace.enable(this.s);

        Column[] columns = TestUtils.generateTableColumns(16);
        Table t = TestUtils.createTable(this.s, columns);

        Collection<PerformanceTrace.Trace> res1 = PerformanceTrace.pop(this.s);
        Collection<PerformanceTrace.Trace> res2 = PerformanceTrace.get(this.s);

        assertEquals(2, res1.size());
        assertEquals(0, res2.size());
    }

    @Test
    public void canCollectBatchPushTraces() throws IOException {
        PerformanceTrace.enable(this.s);

        Column[] columns = TestUtils.generateTableColumns(16);

        Collection<PerformanceTrace.Trace> res1 = PerformanceTrace.pop(this.s);

        Table t = TestUtils.createTable(this.s, columns);

        Collection<PerformanceTrace.Trace> res2 = PerformanceTrace.pop(this.s);

        assertEquals(0, res1.size());
        assertEquals(2, res2.size());

        Writer w = Writer.builder(this.s).build();

        Collection<PerformanceTrace.Trace> res3 = PerformanceTrace.pop(this.s);
        assertEquals(0, res3.size());

        WritableRow[] rows = TestUtils.generateTableRows(columns, 32);
        for (WritableRow row : rows) {
            w.append(t, row);
        }

        Collection<PerformanceTrace.Trace> res4 = PerformanceTrace.pop(this.s);
        assertEquals(0, res4.size());

        w.flush();

        Collection<PerformanceTrace.Trace> res5 = PerformanceTrace.pop(this.s);
        assertEquals(2, res5.size());
    }

    @Test
    public void canLogTraces() throws IOException {
        PerformanceTrace.enable(this.s);

        Column[] columns = TestUtils.generateTableColumns(16);

        Collection<PerformanceTrace.Trace> res1 = PerformanceTrace.pop(this.s);

        Table t = TestUtils.createTable(this.s, columns);

        PerformanceTrace.log(this.s);
    }
}
