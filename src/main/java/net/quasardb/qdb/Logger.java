package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.io.File;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.message.TimestampMessage;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneOffset;

public class Logger
{
    public static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("QdbNative");

    protected static class QdbMessage implements Message, TimestampMessage {
        private final Instant ts;
        private final long pid;
        private final long tid;
        private final String msg;


        public QdbMessage(Instant ts, long pid, long tid, String msg) {
            this.ts  = ts;
            this.pid = pid;
            this.tid = tid;
            this.msg = msg;
        }

        @Override
        public String getFormattedMessage() {
            return this.msg;
        }

        @Override
        public String getFormat() {
            return getFormattedMessage();
        }

        @Override
        public Object[] getParameters() {
            return null;
        }

        @Override
        public String toString() {
            return getFormattedMessage();
        }

        @Override
        public Throwable getThrowable() {
            return null;
        }

        @Override
        public long getTimestamp() {
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
            return Level.FATAL;
        };

        return Level.OFF;
    }

    public static void log(int level,
                           int year, int month, int day,
                           int hour, int min, int sec,
                           long pid, long tid,
                           String msg)  {
        System.out.println("got level: " + level + ", pid = " + pid + ", tid = " + tid);
        System.out.println("got message: " + msg);
        Level l = levelFromNative(level);
        logger.log(l, new QdbMessage(LocalDateTime.of(year, month, day,
                                                      hour, min, sec).toInstant(ZoneOffset.UTC),
                                     pid, tid, msg));
    }
}
