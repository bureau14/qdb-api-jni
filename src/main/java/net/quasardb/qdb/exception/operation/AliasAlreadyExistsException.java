package net.quasardb.qdb.exception;

/**
 * Exception thrown when the specified entry already exists in the database.
 */
public final class AliasAlreadyExistsException extends OperationException {

    public AliasAlreadyExistsException(String message) {
        super(message);
    }
}
