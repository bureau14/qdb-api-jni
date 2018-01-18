package net.quasardb.qdb;

/**
 * Exception thrown when the specified entry already exists in the database.
 */
public final class QdbAliasAlreadyExistsException extends QdbOperationException {

    public QdbAliasAlreadyExistsException() {
        super("An entry matching the provided alias already exists.");
    }
}
