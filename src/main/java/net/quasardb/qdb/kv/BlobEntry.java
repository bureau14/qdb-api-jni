package net.quasardb.qdb.kv;

import java.nio.ByteBuffer;

import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A blob in the database.
 * Blob stands for "Binary Large Object", it's an entry which store binary data.
 */
public final class BlobEntry extends Entry {

    protected BlobEntry(Session session, String alias) {
        super(session, alias);
    }

    public static BlobEntry ofAlias(Session session, String alias) {
        return new BlobEntry(session, alias);
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
        assert(content.isDirect());
        session.throwIfClosed();

        qdb.blob_put(session.handle(), alias, content, -1);
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
    public boolean update(ByteBuffer content) {
        assert(content.isDirect());
        session.throwIfClosed();
        int err = qdb.blob_update(session.handle(), alias, content, -1);
        return err == qdb_error.ok_created;
    }

    /**
      * Read the content of the blob.
      *
      * @return The current content.
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public Buffer get() {
        session.throwIfClosed();
        Reference<ByteBuffer> content = new Reference<ByteBuffer>();
        qdb.blob_get(session.handle(), alias, content);
        return Buffer.wrap(session, content);
    }

}
