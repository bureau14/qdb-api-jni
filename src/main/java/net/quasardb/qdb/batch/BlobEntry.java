package net.quasardb.qdb.batch;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A blob in the database.
 */
public final class BlobEntry {
    private static final Logger logger = LoggerFactory.getLogger(StringEntry.class);

    private Batch batch;
    private String alias;

    protected BlobEntry(Batch batch, String alias) {
        this.batch = batch;
        this.alias = alias;
    }

    public static BlobEntry ofAlias(Batch batch, String alias) {
        return new BlobEntry(batch, alias);
    }

    /**
     * Create a new blob with the specified content. Fails if the blob already exists.
     *
     * @param content The content of the blob to be created.
     */
    public void put(ByteBuffer content) {
        batch.add(new Batch.Operation() {
                @Override
                public void process(long handle, long batch, int index) {
                    logger.info("processing batch blob_put alias = {}, index = {}", alias, index);
                    qdb.batch_write_blob_put(batch, index, alias, content, -1);
                }});
    }


    /**
      * Replaces the content of the blob.
     *
     * @param content The content of the blob to be created.
     */
    public void update(ByteBuffer content) {
        batch.add(new Batch.Operation() {
                @Override
                public void process(long handle, long batch, int index) {
                    logger.info("processing batch blob_update alias = {}, index = {}", alias, index);
                    qdb.batch_write_blob_update(batch, index, alias, content, -1);
                }});
    }


}
