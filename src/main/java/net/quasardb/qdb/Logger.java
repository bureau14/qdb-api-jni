package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.io.File;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.message.TimestampMessage;

public class Logger
{
    public static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("QuasarDB");

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

    public static void log(int level, long pid, long tid, String msg)  {
        Level l = levelFromNative(level);
        logger.log(l, "({}:{}): {}", pid, tid, msg);
    }
}
