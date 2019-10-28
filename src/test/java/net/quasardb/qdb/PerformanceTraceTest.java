package net.quasardb.qdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.*;

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
}
