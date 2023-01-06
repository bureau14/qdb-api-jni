package net.quasardb.qdb.batch;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A integer in the database.
 */
public final class IntegerEntry {
    private static final Logger logger = LoggerFactory.getLogger(IntegerEntry.class);

    private Batch batch;
    private String alias;

    protected IntegerEntry(Batch batch, String alias) {
        this.batch = batch;
        this.alias = alias;
    }

    public static IntegerEntry ofAlias(Batch batch, String alias) {
        return new IntegerEntry(batch, alias);
    }

    /**
     * Create a new integer with the specified content. Fails if the integer already exists.
     *
     * @param content The integer value to be stored.
     */
    public void put(long content) {
        batch.add(new Batch.Operation() {
                @Override
                public void process(long handle, long batch, int index) {
                    logger.info("processing batch integer_put alias = {}, index = {}", alias, index);
                    qdb.batch_write_int_put(handle, batch, index, alias, content, -1);
                }});
    }


    /**
      * Replaces the content of the integer.
     *
     * @param content The integer value to be stored.
     */
    public void update(long content) {
        batch.add(new Batch.Operation() {
                @Override
                public void process(long handle, long batch, int index) {
                    logger.info("processing batch integer_update alias = {}, index = {}", alias, index);
                    qdb.batch_write_int_update(handle, batch, index, alias, content, -1);
                }});
    }


}
