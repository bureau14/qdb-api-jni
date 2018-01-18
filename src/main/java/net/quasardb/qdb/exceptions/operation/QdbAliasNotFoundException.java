package net.quasardb.qdb;

/**
 * Exception thrown when an entry cannot be found in the database
 */
public final class QdbAliasNotFoundException extends QdbOperationException {

    public QdbAliasNotFoundException() {
        super("An entry matching the provided alias cannot be found.");
    }
}
