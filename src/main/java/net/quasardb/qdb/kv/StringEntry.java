package net.quasardb.qdb.kv;

import java.nio.ByteBuffer;

import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A string in the database.
 * Blob stands for "Binary Large Object", it's an entry which store binary data.
 */
public final class StringEntry extends Entry {

    protected StringEntry(Session session, String alias) {
        super(session, alias);
    }

    public static StringEntry ofAlias(Session session, String alias) {
        return new StringEntry(session, alias);
    }

    /**
     * Create a new string with the specified content. Fails if the string already exists.
     *
     * @param content The content of the string to be created.
     * @throws AliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(String content) {
        session.throwIfClosed();

        qdb.string_put(session.handle(), alias, content);
    }

    /**
      * Replaces the content of the string.
      *
      * @param content The content of the string to be set.
      * @return true if the string was created, or false it it was updated.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws InvalidArgumentException If the expiry time is in the past (with a certain tolerance)
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean update(String content) {
        session.throwIfClosed();
        return qdb.string_update(session.handle(), alias, content);
    }

    /**
      * Read the content of the string.
      *
      * @return The current content.
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public String get() {
        session.throwIfClosed();
        return qdb.string_get(session.handle(), alias);
    }

}
