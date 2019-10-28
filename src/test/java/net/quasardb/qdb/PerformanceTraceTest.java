package net.quasardb.qdb;

import java.io.IOException;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.ts.*;

public class PerformanceTraceTest {

    @Test
    public void canEnablePerformanceTrace() {
        Session s = TestUtils.createSession();
        PerformanceTrace.enable(s);
    }


    @Test
    public void canDisablePerformanceTrace() {
        Session s = TestUtils.createSession();
        PerformanceTrace.disable(s);
    }


    @Test
    public void canReenablePerformanceTrace() {
        Session s = TestUtils.createSession();

        PerformanceTrace.enable(s);
        PerformanceTrace.disable(s);
    }


    @Test
    public void canGetEmptyPerformanceTraces() {
        Session s = TestUtils.createSession();

        PerformanceTrace.enable(s);

        Collection<PerformanceTrace.Trace> res = PerformanceTrace.getTraces(s);
        assertEquals(res.size(), 0);
    }

    @Test
    public void canGetTableCreatePerformanceTraces() throws IOException {
        Session s = TestUtils.createSession();

        PerformanceTrace.enable(s);


        Column[] columns = TestUtils.generateTableColumns(16);
        Table t = TestUtils.createTable(s, columns);

        Collection<PerformanceTrace.Trace> res = PerformanceTrace.getTraces(s);
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
}
