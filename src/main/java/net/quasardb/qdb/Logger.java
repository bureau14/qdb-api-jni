package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.io.File;
import java.lang.reflect.Method;

import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.slf4j.event.EventRecodingLogger;
import org.slf4j.helpers.SubstituteLoggerFactory;
import org.slf4j.helpers.SubstituteLogger;

import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneOffset;

public class Logger
{
    private static final org.slf4j.Logger _delegate = LoggerFactory.getLogger("QdbNative");
    private static final Method logMethodCache = logMethod(_delegate);


    /**
     * Ugly hack, but we need to keep alive at least one session handle, otherwise the native logger
     * service might be destroyed when no handles are left.
     */
    private static Session _keepAlive = new Session();

    protected static class QdbEvent implements LoggingEvent {
        private final Instant ts;
        private final long pid;
        private final long tid;
        private final String msg;
        private final Level level;

        public QdbEvent(Level level, Instant ts, long pid, long tid, String msg) {
            this.level = level;
            this.ts    = ts;
            this.pid   = pid;
            this.tid   = tid;
            this.msg   = msg;
        }

        @Override
        public String getMessage() {
            return this.msg;
        }

        @Override
        public Level getLevel() {
            return this.level;
        }

        // @Override
        // public String getFormat() {
        //     return getFormattedMessage();
        // }

        @Override
        public Marker getMarker() {
            return null;
        }

        @Override
        public Object[] getArgumentArray() {
            return null;
        }

        @Override
        public String toString() {
            return getMessage();
        }

        @Override
        public String getLoggerName() {
            return _delegate.getName();
        }

        @Override
        public String getThreadName() {
            return Long.toString(this.tid);
        }

        @Override
        public Throwable getThrowable() {
            return null;
        }

        @Override
        public long getTimeStamp() {
            return this.ts.toEpochMilli();
        }
    };

    public static Level levelFromNative(int level) {
        switch (level) {
        case qdb_log_level.detailed:
            return Level.TRACE;
        case qdb_log_level.debug:
            return Level.DEBUG;
        case qdb_log_level.info:
            return Level.INFO;
        case qdb_log_level.warning:
            return Level.WARN;
        case qdb_log_level.error:
            return Level.ERROR;
        case qdb_log_level.panic:
            return Level.ERROR;
        };

        return null;
    }

    private static Method logMethod(org.slf4j.Logger l) {
        try {
            return l.getClass().getMethod("log", LoggingEvent.class);
        } catch (Exception e) {
            System.err.println("Fatal error: unable to lookup slf4j log method: \n" + e.toString());
        }

        return null;
    }

    private static Instant toInstant(int year, int month, int day,
                                     int hour, int min, int sec) {
        return LocalDateTime.of(year, month, day,
                                hour, min, sec).toInstant(ZoneOffset.UTC);
        _delegate.info(msg);
    }

    public static void warn(String msg) {
        _delegate.warn(msg);
    }

    public static void error(String msg) {
        _delegate.error(msg);
    }
}
