package net.quasardb.qdb.exception;

/**
 * Exception thrown when the specified alias is reserved for quasardb intenal use.
 */
public final class ReservedAliasException extends InputException {

    public ReservedAliasException(String message) {
        super(message);
    }
}
