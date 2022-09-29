package net.quasardb.qdb.kv;

import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A 64-bit floating point in the database.
 */
public final class DoubleEntry extends Entry {

    protected DoubleEntry(Session session, String alias) {
        super(session, alias);
    }

    public static DoubleEntry ofAlias(Session session, String alias) {
        return new DoubleEntry(session, alias);
    }

    /**
     * Create an double with the specified value. Fails if the double already exists.
     *
     * @param value The value of the double to be created.
     * @throws AliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(double value) {
        session.throwIfClosed();

        qdb.double_put(session.handle(), alias, value);
    }

    /**
      * Replaces the content of the double.
      *
      * @param value The double to be set.
      * @return true if the blob was created, or false it it was updated.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws InvalidArgumentException If the expiry time is in the past (with a certain tolerance)
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean update(double value) {
        session.throwIfClosed();
        return qdb.double_update(session.handle(), alias, value);
    }

    /**
      * Read the content of the blob.
      *
      * @return The current content.
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public double get() {
        session.throwIfClosed();
        return qdb.double_get(session.handle(), alias);
    }
}
