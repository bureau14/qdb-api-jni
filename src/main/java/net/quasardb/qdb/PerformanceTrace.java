package net.quasardb.qdb;

import java.util.Collection;
import java.util.Arrays;
import java.util.List;

import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.Reference;

import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.exception.ExceptionFactory;

public class PerformanceTrace {

    public static class Measurement {
        public String label;
        public long elapsed;

        public Measurement(String label, long elapsed) {
            this.label = label;
            this.elapsed = elapsed;
        }
    }

    public static class Trace {
        public String name;
        public Measurement[] measurements;

        public Trace(String name, Measurement[] measurements) {
            this.name = name;
            this.measurements = measurements;
        }
    }

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

    public static Collection<Trace> getTraces(Session s) {
        s.throwIfClosed();

        Reference<Trace[]> result = new Reference<Trace[]>();

        int err = qdb.get_performance_traces(s.handle(), result);
        ExceptionFactory.throwIfError(err);

        return Arrays.asList(result.get());
    }

    public static void clearTraces(Session s) {
        s.throwIfClosed();

        int err = qdb.clear_performance_traces(s.handle());
        ExceptionFactory.throwIfError(err);
    }
}
