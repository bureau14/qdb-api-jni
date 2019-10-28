package net.quasardb.qdb;

import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.exception.ExceptionFactory;

public class PerformanceTrace {

    public static void enable(Session s) {
        s.throwIfClosed();

        int err = qdb.enable_performance_trace(s.handle());
        ExceptionFactory.throwIfError(err);
    }

    public static void disable(Session s) {
        s.throwIfClosed();

        int err = qdb.disable_performance_trace(s.handle());
        ExceptionFactory.throwIfError(err);
    }

}
