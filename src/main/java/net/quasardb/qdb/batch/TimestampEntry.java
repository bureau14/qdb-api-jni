package net.quasardb.qdb.batch;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A timestamp in the database.
 */
public final class TimestampEntry {
    private static final Logger logger = LoggerFactory.getLogger(TimestampEntry.class);

    private Batch batch;
    private String alias;

    protected TimestampEntry(Batch batch, String alias) {
        this.batch = batch;
        this.alias = alias;
    }

    public static TimestampEntry ofAlias(Batch batch, String alias) {
        return new TimestampEntry(batch, alias);
    }

    /**
     * Create a new timestamp with the specified content. Fails if the timestamp already exists.
     *
     * @param content The timestamp value to be stored.
     */
    public void put(Timespec content) {
        batch.add(new Batch.Operation() {
                @Override
                public void process(long handle, long batch, int index) {
                    logger.info("processing batch timestamp_put alias = {}, index = {}", alias, index);
                    qdb.batch_write_timestamp_put(handle, batch, index, alias, content, -1);
                }});
    }


    /**
      * Replaces the content of the timestamp.
     *
     * @param content The timestamp value to be stored.
     */
    public void update(Timespec content) {
        batch.add(new Batch.Operation() {
                @Override
                public void process(long handle, long batch, int index) {
                    logger.info("processing batch timestamp_update alias = {}, index = {}", alias, index);
                    qdb.batch_write_timestamp_update(handle, batch, index, alias, content, -1);
                }});
    }


}
