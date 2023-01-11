package net.quasardb.qdb.batch;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A string in the database.
 */
public final class StringEntry {
    private static final Logger logger = LoggerFactory.getLogger(StringEntry.class);

    private Batch batch;
    private String alias;

    protected StringEntry(Batch batch, String alias) {
        this.batch = batch;
        this.alias = alias;
    }

    public static StringEntry ofAlias(Batch batch, String alias) {
        return new StringEntry(batch, alias);
    }

    /**
     * Create a new string with the specified content. Fails if the string already exists.
     *
     * @param content The content of the string to be created.
     */
    public void put(String content) {
        batch.add(new Batch.Operation() {
                @Override
                public void process(long handle, long batch, int index) {
                    logger.debug("processing batch string_put = {}, index = {}", alias, index);
                    qdb.batch_write_string_put(handle, batch, index, alias, content, -1);
                }});
    }


    /**
      * Replaces the content of the string.
     *
     * @param content The content of the string to be stored.
     */
    public void update(String content) {
        batch.add(new Batch.Operation() {
                @Override
                public void process(long handle, long batch, int index) {
                    logger.debug("processing batch string_update alias = {}, index = {}", alias, index);
                    qdb.batch_write_string_update(handle, batch, index, alias, content, -1);
                }});
    }


}
