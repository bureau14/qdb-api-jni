package net.quasardb.qdb.batch;

import java.nio.ByteBuffer;

import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A blob in the database.
 */
public final class BlobEntry {

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
     * @throws AliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(ByteBuffer content) {
        batch.add(new Batch.Operation() {
                @Override
                public void process(long handle, long batch, int index) {
                    qdb.batch_write_blob_put(batch, index, alias, content, -1);
                }});
    }

    /**
      * Replaces the content of the blob.
      *
      * @param content The content of the blob to be set.
      * @return true if the blob was created, or false it it was updated.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws InvalidArgumentException If the expiry time is in the past (with a certain tolerance)
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public void update(ByteBuffer content) {

    }

    /**
      * Read the content of the blob.
      *
      * @return The current content.
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public void get() {
    }

}
