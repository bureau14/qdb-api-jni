package net.quasardb.qdb.exception;

/**
 * Exception thrown when an entry cannot be found in the database
 */
public final class AliasNotFoundException extends OperationException {

    public AliasNotFoundException(String message) {
        super(message);
    }
}
