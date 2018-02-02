package net.quasardb.qdb.exception;

/**
 * Exception thrown when an entry cannot be found in the database
 */
public final class AliasNotFoundException extends OperationException {

    public AliasNotFoundException() {
        super("An entry matching the provided alias cannot be found.");
    }
}
