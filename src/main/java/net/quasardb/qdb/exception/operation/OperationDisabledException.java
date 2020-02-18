package net.quasardb.qdb.exception;

/**
 * Exception thrown when the operation cannot be performed because it has been disabled.
 */
public final class OperationDisabledException extends OperationException {

    public OperationDisabledException(String message) {
        super(message);
    }
}
