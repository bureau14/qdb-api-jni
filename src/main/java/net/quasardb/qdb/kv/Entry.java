package net.quasardb.qdb.kv;

import net.quasardb.qdb.Session;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A blob in the database.
 * Blob stands for "Binary Large Object", it's an entry which store binary data.
 */
public class Entry {

    public enum Type {
        UNINITIALIZED(Constants.qdb_ts_column_uninitialized),
        DOUBLE(Constants.qdb_ts_column_double),
        BLOB(Constants.qdb_ts_column_blob),
        STRING(Constants.qdb_ts_column_string),
        INT64(Constants.qdb_ts_column_int64),
        TIMESTAMP(Constants.qdb_ts_column_timestamp)
        ;

        protected final int value;
        Type(int type) {
            this.value = type;
        }

        public int asInt() {
            return this.value;
        }

        public static Type fromInt(int type) {
            switch(type) {
            case Constants.qdb_ts_column_double:
                return Type.DOUBLE;

            case Constants.qdb_ts_column_blob:
                return Type.BLOB;

            case Constants.qdb_ts_column_string:
                return Type.STRING;

            case Constants.qdb_ts_column_int64:
                return Type.INT64;

            case Constants.qdb_ts_column_timestamp:
                return Type.TIMESTAMP;
            }


            return Type.UNINITIALIZED;
        }
    }

    protected Session session;
    protected String alias;

    /**
     * Constructor.
     *
     * @param session Active connection to the QuasarDB cluster.
     * @param alias Alias / key of the entry
     */
    protected Entry(Session session, String alias) {
        this.session = session;
        this.alias = alias;
    }

    /**
     * Gets the alias (i.e. its "key") of the entry in the database.
     *
     * @return The alias.
     */
    public String alias() {
        return alias;
    }

    /**
      * Attaches a tag to the entry. The tag is created if it does not exist.
      *
      * @param tag The alias of the tag to attach.
      * @return true if the tag has been attached, false if it was already attached
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean attachTag(String tag) {
        session.throwIfClosed();
        int err = qdb.attach_tag(session.handle(), alias, tag);
        return err != qdb_error.tag_already_set;
    }

    /**
      * Checks if a tag is attached to the entry.
      *
      * @param tag The alias to the tag to check.
      * @return true if the entry has the provided tag, false otherwise
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean hasTag(String tag) {
        session.throwIfClosed();
        int err = qdb.has_tag(session.handle(), alias, tag);
        return err != qdb_error.tag_not_set;
    }

    /**
      * Removes the entry from the database.
      *
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public void remove() {
        session.throwIfClosed();
        qdb.remove(session.handle(), alias);
    }

    /**
      * Detaches a tag from the entry.
      *
      * @param tag The alias of the tag to detach.
      * @return true if the tag has been detached, false if the tag was not attached
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean detachTag(String tag) {
        session.throwIfClosed();
        int err = qdb.detach_tag(session.handle(), alias, tag);
        return err != qdb_error.tag_not_set;
    }

    /**
     * Check if the entry exists.
     *
     * @return Returns true if the entry exists, false otherwise.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias is reserved for quasardb internal use.
     */
    public boolean exists() {
        session.throwIfClosed();
        return qdb.entry_exists(session.handle(), alias);
    }
}
