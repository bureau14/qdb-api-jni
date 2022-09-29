package net.quasardb.qdb.kv;

import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A 64-bit floating point in the database.
 */
public final class TimestampEntry extends Entry {

    protected TimestampEntry(Session session, String alias) {
        super(session, alias);
    }

    public static TimestampEntry ofAlias(Session session, String alias) {
        return new TimestampEntry(session, alias);
    }

    /**
     * Create an timestamp with the specified value. Fails if the timestamp already exists.
     *
     * @param value The value of the timestamp to be created.
     * @throws AliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(Timespec value) {
        session.throwIfClosed();

        qdb.timestamp_put(session.handle(), alias, value);
    }

    /**
      * Replaces the content of the timestamp.
      *
      * @param value The timestamp to be set.
      * @return true if the blob was created, or false it it was updated.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws InvalidArgumentException If the expiry time is in the past (with a certain tolerance)
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean update(Timespec value) {
        session.throwIfClosed();
        return qdb.timestamp_update(session.handle(), alias, value);
    }

    /**
      * Read the content of the blob.
      *
      * @return The current content.
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public Timespec get() {
        session.throwIfClosed();
        return qdb.timestamp_get(session.handle(), alias);
    }
}
