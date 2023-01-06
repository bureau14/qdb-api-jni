package net.quasardb.qdb.batch;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A double in the database.
 */
public final class DoubleEntry {
    private static final Logger logger = LoggerFactory.getLogger(DoubleEntry.class);

    private Batch batch;
    private String alias;

    protected DoubleEntry(Batch batch, String alias) {
        this.batch = batch;
        this.alias = alias;
    }

    public static DoubleEntry ofAlias(Batch batch, String alias) {
        return new DoubleEntry(batch, alias);
    }

    /**
     * Create a new double with the specified content. Fails if the double already exists.
     *
     * @param content The double value to be stored.
     */
    public void put(double content) {
        batch.add(new Batch.Operation() {
                @Override
                public void process(long handle, long batch, int index) {
                    logger.info("processing batch double_put alias = {}, index = {}", alias, index);
                    qdb.batch_write_double_put(handle, batch, index, alias, content, -1);
                }});
    }


    /**
      * Replaces the content of the double.
     *
     * @param content The double value to be stored.
     */
    public void update(double content) {
        batch.add(new Batch.Operation() {
                @Override
                public void process(long handle, long batch, int index) {
                    logger.info("processing batch double_update alias = {}, index = {}", alias, index);
                    qdb.batch_write_double_update(handle, batch, index, alias, content, -1);
                }});
    }


}
