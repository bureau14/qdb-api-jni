package net.quasardb.qdb.ts;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.SeekableByteChannel;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a timeseries row.
 */
public final class Query implements Serializable {

    private String query;

    public static void of(String query) {
        this.query = query;
    }

    public void execute(QdbSession session) {
        return execute(QdbSession session, QdbTimeSeriesQueryResult
    }

}
