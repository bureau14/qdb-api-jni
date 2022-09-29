package net.quasardb.qdb.kv;

import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A 64-bit integer in the database.
 */
public final class IntegerEntry extends Entry {

    protected IntegerEntry(Session session, String alias) {
        super(session, alias);
    }

    public static IntegerEntry ofAlias(Session session, String alias) {
        return new IntegerEntry(session, alias);
    }

    /**
     * Create an integer with the specified value. Fails if the integer already exists.
     *
     * @param value The value of the integer to be created.
     * @throws AliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(long value) {
        session.throwIfClosed();

        qdb.int_put(session.handle(), alias, value, -1);
    }

    /**
      * Replaces the content of the integer.
      *
      * @param value The integer to be set.
      * @return true if the blob was created, or false it it was updated.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws InvalidArgumentException If the expiry time is in the past (with a certain tolerance)
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean update(long value) {
        session.throwIfClosed();
        int err = qdb.int_update(session.handle(), alias, value, -1);
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
    public long get() {
        session.throwIfClosed();
        Reference<Long> value = new Reference<Long>();
        qdb.int_get(session.handle(), alias, value);
        return value.value;
    }
}
